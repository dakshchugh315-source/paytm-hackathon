"""End-to-end tests for Saathi: ML engine, merchant/customer flows, guardrails, audit, persistence, consoles, Soundbox, UI."""
import os, re, shutil, subprocess, time
import pytest
import main, ml_engine, generate_dataset
from conftest import chat, approve, PIN

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------- ML engine & dataset ----------------
def test_dataset_shape_and_labels():
    rows = generate_dataset.generate(300)
    assert len(rows) == 300
    assert set(rows[0]) >= {"persona", "lang", "issue_type", "txn_amount", "user_sentiment_score", "past_dispute_count", "bank_server_status", "target_action", "utterance"}
    assert {r["lang"] for r in rows} == set(generate_dataset.LANGS)
    assert all(10 <= r["txn_amount"] <= 50000 and 0 <= r["user_sentiment_score"] <= 1 for r in rows)

def test_model_quality_report():
    rep = ml_engine.load()["report"]
    assert rep["issue_acc"] >= 0.9 and rep["action_acc"] >= 0.9

@pytest.mark.parametrize("text,issue,action", [
    ("Maine ₹420 bheje, paisa kat gaya par dukaan wale ko nahi mila", "UPI_Pending", "AUTO_REFUND_INSTANT"),
    ("என் EMI கட்ட பயமா இருக்கு, late fee வருமா?", "EMI_Bounce_Fear", "SEND_EMI_LINK_WITH_REASSURANCE"),
    ("I did NOT make this ₹12,000 payment!! someone stole my money", "Fraud_Unauthorized_Txn", "FREEZE_AND_HUMAN_HANDOFF"),
    ("Mere regular customers ko Diwali offer bhej do", "Festival_Promo_Broadcast", "TRIGGER_WHATSAPP_CAMPAIGN"),
    ("Subah se ₹8,500 ka settlement hold pe hai", "Settlement_Hold", "RELEASE_INSTANT_SETTLEMENT"),
    ("hello boss my last transaction was failed soo can u pls refund that", "UPI_Pending", "AUTO_REFUND_INSTANT"),
])
def test_ml_decisions(text, issue, action):
    d = ml_engine.decide(text, bank="DEGRADED")
    assert (d["issue"], d["action"]) == (issue, action)

def test_ml_latency_and_language():
    ml_engine.decide("warm up")
    t = time.perf_counter(); [ml_engine.decide("Kal ka settlement nahi aaya") for _ in range(20)]
    assert (time.perf_counter() - t) / 20 < 0.05
    assert ml_engine.detect_lang("என் EMI") == "Tamil" and ml_engine.detect_lang("my payment failed") == "English"
    assert ml_engine.detect_lang("paisa kat gaya") == "Hinglish" and ml_engine.detect_lang("मी पैसे पाठवले आहे") == "Marathi"

def test_inr_and_mask():
    assert main.inr(190000) == "₹1,90,000" and main.inr(420) == "₹420"
    assert main.mask("acc 50100234567890 ifsc HDFC0001234") == "acc XXXX-7890 ifsc HDFC0XXXXXX"

# ---------------- Merchant flows ----------------
def test_settlement_bank_change_needs_approval_then_verifies(c):
    r = chat(c, "Kal ka settlement nahi aaya")
    assert r["status"] == "WAITING_APPROVAL" and [a["tool"] for a in r["approvals"]] == ["trigger_resettlement"]
    r = approve(c, r)
    assert r["status"] == "VERIFIED" and "UTR" in r["messages"][-2]["text"]
    assert main.MERCHANTS["M1001"]["settlements"]["ST-9001"]["status"] == "SETTLED"

def test_instant_settlement_low_risk(c):
    r = chat(c, "Subah se ₹8,500 ka settlement hold pe hai", "M1002")
    assert r["status"] == "VERIFIED" and r["diff"]["before"]["status"] == "ON_HOLD" and r["diff"]["after"]["status"] == "SETTLED"

def test_soundbox_autoresolves_and_cache_hit(c):
    c.post("/demo/soundbox_offline")
    assert chat(c, "Soundbox bol nahi raha")["status"] == "VERIFIED"
    c.post("/demo/soundbox_offline")
    r = chat(c, "Soundbox bol nahi raha")
    assert r["cache_hit"] and main.DEVICES["SB-7781"]["online"]

def test_failed_payment_asks_which_then_refunds_only_that(c):
    r = chat(c, "Customer ka paisa kat gaya, payment fail")
    assert r["status"] == "NEEDS_INFO" and len(r["messages"][-1]["options"]) == 2
    r = chat(c, r["messages"][-1]["options"][0])
    tx = main.MERCHANTS["M1001"]["txns"]
    assert tx["430711225511"]["refunded"] and not tx["430711225512"]["refunded"]

def test_big_refund_needs_approval(c):
    chat(c, "Customer ka paisa kat gaya, payment fail")
    r = chat(c, "09:47 wala ₹1,200 refund karo")
    assert [a["tool"] for a in r["approvals"]] == ["process_refund"]
    approve(c, r); assert main.MERCHANTS["M1001"]["txns"]["430711225512"]["refunded"]

def test_unclear_message_never_acts(c):
    r = chat(c, "mere customer ne mujhe paise nahi diye")
    assert r["status"] == "NEEDS_INFO" and r["messages"][-1].get("options")
    assert not any(t["refunded"] for t in main.MERCHANTS["M1001"]["txns"].values())

def test_kyc_bank_change_flow(c):
    r = chat(c, "Mujhe bank account badalna hai")
    assert [a["tool"] for a in r["approvals"]] == ["update_bank_account"]
    assert approve(c, r)["status"] == "VERIFIED" and main.MERCHANTS["M1001"]["bank"]["acc"] == "60200987654321"

def test_proactive_webhook_no_duplicates(c):
    r1 = c.post("/webhook/settlement_failed?merchant_id=M1001").json()
    r2 = c.post("/webhook/settlement_failed?merchant_id=M1001").json()
    assert "Namaste Ramesh ji" in [m for m in r1["messages"] if m["from"] == "saathi"][0]["text"]
    assert len([a for a in r2["approvals"] if a["tool"] == "trigger_resettlement"]) == 1
    r = c.post("/webhook/settlement_failed?merchant_id=M1002").json()  # ML instant-release path greets too
    assert "Namaste Sunita ji" in [m for m in r["messages"] if m["from"] == "saathi"][0]["text"]

def test_campaign_needs_merchant_consent(c):
    r = chat(c, "Mere regular customers ko Diwali offer bhej do", "M1002")
    assert r["messages"][-1]["cta"]["product"] == "CAMPAIGN" and not main.CAMPAIGNS
    r = c.post(f"/lead/{r['session_id']}?product=CAMPAIGN").json()
    assert len(main.CAMPAIGNS) == 1 and any(m.get("campaign") for m in r["messages"])
    assert not any("₹1 lakh" in m["text"] for m in r["messages"])  # ₹1,90,000 offer must not be shown as "1 lakh"

def test_sales_pivot_once_and_orders_ledger(c):
    r = approve(c, chat(c, "Kal ka settlement nahi aaya"))
    pitch = [m for m in r["messages"] if m.get("kind") == "pitch"]
    assert len(pitch) == 1
    r = c.post(f"/lead/{r['session_id']}?product=Business%20Loan&amount=100000").json()
    o = r["profile"]["orders"][0]
    assert o["direction"] == "CREDIT" and o["item"] == "Business Loan ₹1,00,000" and "₹317/din" in r["messages"][-1]["text"]
    c.post("/demo/soundbox_offline")
    assert not [m for m in chat(c, "Soundbox bol nahi raha")["messages"] if m.get("kind") == "pitch"][1:]

# ---------------- Customer flows ----------------
def test_customer_upi_refund(c):
    r = chat(c, "Maine ₹420 bheje the, paisa kat gaya par dukaan wale ko nahi mila", "C2001")
    assert r["status"] == "VERIFIED" and main.CUSTOMERS["C2001"]["txns"]["UPI-48821"]["status"] == "REFUNDED"

def test_customer_emi_waiver_and_pay(c):
    r = chat(c, "என் EMI கட்ட பயமா இருக்கு, late fee வருமா?", "C2002")
    assert main.CUSTOMERS["C2002"]["emi"]["waived"] and r["messages"][-1]["cta"]["product"] == "EMI"
    r = c.post(f"/lead/{r['session_id']}?product=EMI&amount=2150").json()
    assert r["status"] == "VERIFIED" and main.CUSTOMERS["C2002"]["emi"]["paid"]

def test_emi_never_charged_twice(c):
    r = chat(c, "Meri EMI bounce ho jayegi kya, late fee lagegi?", "C2002"); first = r["session_id"]
    c.post(f"/lead/{first}?product=EMI&amount=2150")
    r = chat(c, "என் EMI கட்ட பயமா இருக்கு, late fee வருமா?", "C2002")
    assert not r["messages"][-1].get("cta") and r["status"] == "VERIFIED" and "pehle hi" in r["messages"][-1]["text"]
    r = c.post(f"/lead/{first}?product=EMI&amount=2150").json()  # stale Pay button pressed again
    assert "Dobara koi paisa nahi" in r["messages"][-1]["text"]
    assert c.post(f"/lead/{first}?product=decline").status_code == 200  # customer 'Abhi nahi' must not crash

def test_fraud_freezes_and_hands_off(c):
    r = chat(c, "I did NOT make this ₹12,000 payment!! someone stole my money", "C2003")
    assert main.CUSTOMERS["C2003"]["frozen"] and [a["tool"] for a in r["approvals"]] == ["fraud_case_handoff"]
    assert "₹12,000" in r["messages"][-1]["text"]

def test_plain_refund_request_never_freezes(c):
    r = chat(c, "hello boss my last transaction was failed soo can u pls refund that", "C2003")
    assert not main.CUSTOMERS["C2003"]["frozen"] and r["status"] == "NEEDS_INFO"

# ---------------- Guardrails, audit, persistence ----------------
def test_pin_required(c):
    r = chat(c, "Kal ka settlement nahi aaya"); aid = r["approvals"][0]["id"]
    assert c.post(f"/approvals/{aid}/approve").status_code == 401
    assert c.post(f"/approvals/{aid}/approve", headers={"x-approver-pin": "0000"}).status_code == 401
    assert c.post("/api/pin-check", headers=PIN).json()["ok"]

def test_idempotent_money_moves(c):
    approve(c, chat(c, "Kal ka settlement nahi aaya"))
    assert "IDEMPOTENT_BLOCK" in main.trigger_resettlement("M1001", "ST-9001", 18450)["error"]

def test_audit_chain_append_only_and_masked(c):
    chat(c, "Kal ka settlement nahi aaya")
    st = c.get("/state").json()
    assert st["chain_ok"]
    with pytest.raises(Exception): main.db.execute("UPDATE audit_chain SET event='x'")
    assert not any("50100234567890" in a["detail"] or "9876543210" in a["detail"] for a in st["audit"])

def test_approvals_survive_restart(c):
    r = chat(c, "Kal ka settlement nahi aaya"); aid = r["approvals"][0]["id"]
    main.SESSIONS.clear(); main.APPROVALS.clear(); main.load_world()
    assert aid in main.APPROVALS
    assert c.post(f"/approvals/{aid}/approve", headers=PIN).json()["status"] == "VERIFIED"

def test_followup_and_negative_csat(c):
    c.post("/demo/soundbox_offline"); chat(c, "Soundbox bol nahi raha")
    r = c.post("/followup").json(); tid = [m for m in r["messages"] if m.get("csat")][-1]["csat"]
    assert r["status"] == "CLOSED"
    r = c.post(f"/csat/{tid}/0").json()
    assert any(a["tool"] == "assign_senior_callback" for a in r["approvals"])

# ---------------- Consoles ----------------
def test_agent_override_and_manual_close(c):
    r = chat(c, "mujhe kuch problem hai paise ki", "C2002")
    r = c.post(f"/api/tickets/{r['session_id']}/override?issue=EMI_Bounce_Fear").json()
    assert r["ml"]["forced_by"] and r["ml"]["action"] == "SEND_EMI_LINK_WITH_REASSURANCE"
    assert c.post(f"/api/tickets/{r['session_id']}/close").json()["status"] == "CLOSED"
    st = c.get("/state?mid=M1001&agent=C2002").json()
    assert st["agent_profile"]["name"] == "Lakshmi R" and any(t["forced"] for t in st["tickets"])

def test_directory_and_executive_metrics(c):
    chat(c, "Subah se ₹8,500 ka settlement hold pe hai", "M1002")
    st = c.get("/state").json()
    assert {u["id"] for u in st["merchants"]} == {"M1001", "M1002", "M1003", "C2001", "C2002", "C2003"}
    b = st["exec"]["biz"]
    assert b["gmv_month"] == 785000 and b["held"] == 18450 and len(st["exec"]["merchant_rows"]) == 3

# ---------------- Voice & Soundbox ----------------
def test_voice_note_uses_transcript(c, monkeypatch):
    monkeypatch.setattr(main, "stt", lambda audio, fname="": "कल का सेटलमेंट नहीं आया")
    r = c.post("/voice", files={"file": ("n.webm", b"x")}, data={"merchant_id": "M1001"}).json()
    assert r["messages"][-2 if r["messages"][-1]["from"] == "saathi" else -1]["text"].startswith("🎤") or any("🎤" in m["text"] for m in r["messages"])
    monkeypatch.setattr(main, "stt", lambda audio, fname="": None)
    assert c.post("/voice", files={"file": ("n.webm", b"x")}, data={"merchant_id": "M1001"}).status_code == 422

def test_soundbox_cash_and_announcements(c, monkeypatch):
    r = c.post("/soundbox/M1002/cash", data={"text": "150 rupaye cash"}).json()
    assert r["entry"]["amount"] == 150 and main.MERCHANTS["M1002"]["cash"][0]["amount"] == 150
    monkeypatch.setattr(main, "stt", lambda audio, fname="": "डेढ़ सौ रुपये कैश")
    assert c.post("/soundbox/M1002/cash", files={"file": ("c.webm", b"x")}).json()["entry"]["amount"] == 150
    assert "prapt hue" in c.post("/soundbox/M1002/simulate-payment").json()["text"]
    assert c.post("/soundbox/M1001/simulate-payment").status_code == 409  # Ramesh's Soundbox is offline
    assert c.post("/soundbox/C2001/cash", data={"text": "100"}).status_code == 404

# ---------------- Staff WhatsApp (internal teammate) ----------------
def staff(c, phone, text):
    return c.post("/staff/chat", json={"phone": phone, "text": text}).json()["chats"][phone]["msgs"]

def test_staff_unknown_number_denied(c):
    msgs = staff(c, "9999999999", "show open tickets")
    assert "employee list mein nahi" in msgs[-1]["text"] and not c.get("/staff/state").json()["chats"]["9999999999"]["verified"]

def test_staff_verified_lists_tickets_and_status(c):
    chat(c, "Kal ka settlement nahi aaya")
    msgs = staff(c, "9811000001", "show open tickets")
    assert "Verified ✅ Priya Sharma" in msgs[-2]["text"] and "Ramesh" in msgs[-1]["text"]
    assert "Gupta Medicals" in staff(c, "9811000001", "status of Sunita")[-1]["text"]
    assert "Aaj ka summary" in staff(c, "9811000001", "today's summary")[-1]["text"]

def test_staff_ping_then_approve_with_pin(c):
    staff(c, "9811000002", "ping me")
    r = chat(c, "Kal ka settlement nahi aaya"); aid = r["approvals"][0]["id"]
    alert = c.get("/staff/state").json()["chats"]["9811000002"]["msgs"][-1]
    assert alert.get("alert") and aid in alert["text"]
    assert "PIN chahiye" in staff(c, "9811000002", f"approve {aid}")[-1]["text"]
    assert "✅ Approved" in staff(c, "9811000002", f"approve {aid} {main.APPROVER_PIN}")[-1]["text"]
    assert main.APPROVALS[aid]["status"] == "APPROVED" and main.MERCHANTS["M1001"]["settlements"]["ST-9001"]["status"] == "SETTLED"

def test_staff_recent_tickets_ordinal_and_id_lookup(c):
    c.post("/demo/soundbox_offline"); done = chat(c, "Soundbox bol nahi raha")["session_id"]  # closed/verified ticket
    chat(c, "Meri EMI bounce ho jayegi kya, late fee lagegi?", "C2002"); chat(c, "Kal ka settlement nahi aaya")
    msgs = staff(c, "9811000001", "give me status of last 2 tickets")
    assert msgs[-1]["text"].startswith("Last 2 ticket") and "1. " in msgs[-1]["text"]
    assert "Pichhli list mein sirf 2" in staff(c, "9811000001", "3rd?")[-1]["text"]
    assert "Lakshmi" in staff(c, "9811000001", "2nd")[-1]["text"]
    detail = staff(c, "9811000001", f"{done} iska bta")[-1]["text"]
    assert done in detail and "VERIFIED" in detail and "Saathi's last reply" in detail
    assert "koi ticket nahi mila" in staff(c, "9811000001", "TK-ZZZZZZ ka status")[-1]["text"]

def test_ticket_issue_reflects_what_was_acted_on(c):
    r = chat(c, "349 की ट्रांजैक्शन दे दे")
    row = next(t for t in c.get("/state").json()["tickets"] if t["id"] == r["session_id"])
    assert row["issue"] != "EMI_Bounce_Fear"

def test_staff_without_rights_cannot_approve(c):
    r = chat(c, "Kal ka settlement nahi aaya"); aid = r["approvals"][0]["id"]
    assert "access nahi" in staff(c, "9811000003", f"approve {aid} {main.APPROVER_PIN}")[-1]["text"]
    assert main.APPROVALS[aid]["status"] == "PENDING"

# ---------------- Frontend sanity ----------------
def test_frontend_structure_and_js_syntax(tmp_path):
    html = open(os.path.join(ROOT, "frontend", "index.html"), encoding="utf-8").read()
    body = html[html.index("<body"):html.index("<script>\n")]
    assert body.count("<div") == body.count("</div>")
    for el in ("app-wa", "app-pt", "app-sb", "queue", "tickets", "dir", "xmoney", "pin", "schat", "semps", "askdrawer", "dchat"): assert f'id="{el}"' in html
    assert 'id="mic" onclick="mic()"' not in html  # form-scope shadowing bug must not come back
    if not shutil.which("node"): pytest.skip("node not installed")
    js = tmp_path / "app.js"; js.write_text(re.findall(r"<script>\n(.*)</script>", html, re.S)[0], encoding="utf-8")
    assert subprocess.run(["node", "--check", str(js)], capture_output=True).returncode == 0

# ---------------- Optional: live LLM ----------------
@pytest.mark.skipif(not os.getenv("RUN_LIVE"), reason="set RUN_LIVE=1 to hit the real LLM/Sarvam")
def test_live_agent_settlement(c):
    r = chat(c, "Kal ka settlement nahi aaya")
    assert r["status"] == "WAITING_APPROVAL" and approve(c, r)["status"] == "VERIFIED"
