"""Saathi — autonomous support & sales teammate for Paytm merchants (Sarvam AI). Team Confused_Devs.
Run: pip install fastapi uvicorn httpx python-multipart && uvicorn main:app --reload
Env: SARVAM_API_KEY (optional), SAATHI_DB_URL=postgresql://... (optional; SQLite otherwise),
     APPROVER_PIN (default 2468), FOLLOWUP_AFTER_S (default 86400 = real 24h scheduler)."""
import os, re, json, time, uuid, sqlite3, hashlib, base64, math, struct, threading, statistics, copy
from datetime import datetime
import httpx, asyncio
from concurrent.futures import ThreadPoolExecutor
try: import edge_tts
except ImportError: edge_tts = None
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ml_engine

for _e in (os.path.join(os.path.dirname(__file__), "..", ".env"), os.path.join(os.path.dirname(__file__), ".env")):
    if os.path.exists(_e):
        for _l in open(_e, encoding="utf-8"):
            if "=" in _l and not _l.lstrip().startswith("#"):
                _parts = _l.split("=", 1)
                os.environ.setdefault(_parts[0].strip(), _parts[1].strip().strip('"').strip("'"))
SARVAM_KEY = os.getenv("SARVAM_API_KEY", "")
PROVIDERS = {"sarvam": ("https://api.sarvam.ai/v1", "sarvam-105b-conversations"),
             "gemini": ("https://generativelanguage.googleapis.com/v1beta/openai", "gemini-2.5-flash"),
             "grok": ("https://api.x.ai/v1", "grok-3-mini"),
             "groq": ("https://api.groq.com/openai/v1", "openai/gpt-oss-120b"),
             "openai": ("https://api.openai.com/v1", "gpt-4o-mini")}
# Fallback chain: first provider with a key is primary; on rate-limit/quota/outage the next one takes over.
LLM_CHAIN = []
for _p in os.getenv("LLM_PROVIDERS", "sarvam,grok,gemini,groq,openai").lower().split(","):
    _p = _p.strip(); _k = os.getenv(f"{_p.upper()}_API_KEY", "")
    if _p in PROVIDERS and _k: LLM_CHAIN.append({"name": _p, "base": PROVIDERS[_p][0], "model": os.getenv(f"{_p.upper()}_MODEL", PROVIDERS[_p][1]), "key": _k, "cool_until": 0})
LLM_KEY = bool(LLM_CHAIN)
LLM_PROVIDER, LLM_MODEL = (LLM_CHAIN[0]["name"], LLM_CHAIN[0]["model"]) if LLM_CHAIN else ("none", "none")
SARVAM = "https://api.sarvam.ai"
AUTO_LIMIT = 500
DAILY_AUTO_CAP = 2000     # ₹ auto-refund ceiling per merchant per day
APPROVER = "agent-01 (Priya)"
APPROVER_PIN = os.getenv("APPROVER_PIN", "2468")
FOLLOWUP_AFTER_S = int(os.getenv("FOLLOWUP_AFTER_S", "86400"))
HUMAN_COST = 45.0          # ₹ per human-handled ticket (pilot baseline)
INR_PER_1K_TOKENS = 0.9    # blended Sarvam-M cost
HUMAN_MIN_COST = 6.0       # ₹ of agent time per one-click approval
app = FastAPI(title="Saathi")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
LOCK = threading.Lock()
PHASES = ["UNDERSTAND", "DIAGNOSE", "ACT", "VERIFY", "HUMAN", "FOLLOWUP"]

# ---------------- State: PostgreSQL (if SAATHI_DB_URL) else SQLite ----------------
class DB:
    def __init__(self, url):
        self.pg = False
        if url.startswith("postgres"):
            try:
                import psycopg; self.c = psycopg.connect(url, autocommit=True); self.pg = True
            except Exception as e: print("Postgres unavailable, falling back to SQLite:", e)
        if not self.pg: self.c = sqlite3.connect(os.getenv("SAATHI_SQLITE") or os.path.join(os.path.dirname(__file__), "saathi.db"), check_same_thread=False)
        self.kind = "PostgreSQL" if self.pg else "SQLite"
    def execute(self, sql, p=()): return self.c.execute(sql.replace("?", "%s") if self.pg else sql, p)
    def commit(self): self.pg or self.c.commit()

db = DB(os.getenv("SAATHI_DB_URL", ""))
AUTO_ID = "BIGSERIAL PRIMARY KEY" if db.pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
db.execute(f"CREATE TABLE IF NOT EXISTS audit_chain(id {AUTO_ID}, ts TEXT, ticket TEXT, phase TEXT, event TEXT, detail TEXT, prev TEXT, hash TEXT)")
db.execute("CREATE TABLE IF NOT EXISTS tix(id TEXT PRIMARY KEY, merchant TEXT, intent TEXT, status TEXT, auto INT, cache INT, tokens INT, cost REAL, opened REAL, resolved REAL)")
db.execute("CREATE TABLE IF NOT EXISTS leads(ts TEXT, merchant TEXT, product TEXT, amount INT)")
db.execute("CREATE TABLE IF NOT EXISTS csat(ts TEXT, ticket TEXT, score INT)")
db.execute("CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT)")

def lock_audit():  # append-only: block UPDATE and DELETE at the database level
    if db.pg:
        db.execute("CREATE OR REPLACE FUNCTION audit_ro() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'audit is append-only'; END $$ LANGUAGE plpgsql")
        db.execute("DROP TRIGGER IF EXISTS audit_immutable ON audit_chain")
        db.execute("CREATE TRIGGER audit_immutable BEFORE UPDATE OR DELETE ON audit_chain FOR EACH ROW EXECUTE FUNCTION audit_ro()")
    else:
        for op in ("UPDATE", "DELETE"):
            db.execute(f"CREATE TRIGGER IF NOT EXISTS audit_no_{op.lower()} BEFORE {op} ON audit_chain BEGIN SELECT RAISE(ABORT,'audit is append-only'); END")
lock_audit(); db.commit()
_last = db.execute("SELECT hash FROM audit_chain ORDER BY id DESC LIMIT 1").fetchone()
LAST_HASH = _last[0] if _last else "GENESIS"

def verify_chain():
    prev = "GENESIS"
    for ts, t, ph, ev, d, pv, h in db.execute("SELECT ts, ticket, phase, event, detail, prev, hash FROM audit_chain ORDER BY id").fetchall():
        if pv != prev or h != hashlib.sha256(f"{prev}|{ts}|{t}|{ph}|{ev}|{d}".encode()).hexdigest(): return False
        prev = h
    return True

SEED = {
    "M1001": {"name": "Sharma General Store", "owner": "Ramesh Sharma", "city": "Delhi", "phone": "9876543210", "monthly_volume": 260000, "has_loan": False, "soundbox": "SB-7781",
              "bank": {"acc": "50100234567890", "ifsc": "HDFC0001234", "changed_days_ago": 1, "verified": False},
              "settlements": {"ST-9001": {"amount": 18450, "date": "yesterday", "status": "FAILED", "reason": "Beneficiary account mismatch (bank changed)"}},
              "txns": {"430711225511": {"amount": 349, "time": "Today 10:32", "payer": "rahul.k@okaxis", "status": "FAILED", "debited": True, "refunded": False},
                       "430711225512": {"amount": 1200, "time": "Today 09:47", "payer": "priya88@ybl", "status": "FAILED", "debited": True, "refunded": False},
                       "430711225498": {"amount": 560, "time": "Today 09:15", "payer": "amit.s@paytm", "status": "SUCCESS", "debited": True, "refunded": False}}},
    "M1002": {"name": "Gupta Medicals", "owner": "Sunita Gupta", "city": "Jaipur", "phone": "9812345670", "monthly_volume": 480000, "has_loan": False, "soundbox": "SB-8820",
              "bank": {"acc": "33090011223344", "ifsc": "SBIN0004321", "changed_days_ago": 90, "verified": True},
              "settlements": {"ST-9102": {"amount": 42300, "date": "yesterday", "status": "SETTLED", "reason": "", "utr": "UTR8821AB09"},
                              "ST-9105": {"amount": 8500, "date": "today 09:00", "status": "ON_HOLD", "reason": "Auto risk-review (velocity spike)"}},
              "txns": {"430722310045": {"amount": 2450, "time": "Today 11:05", "payer": "dr.mehta@icici", "status": "FAILED", "debited": True, "refunded": False},
                       "430722310031": {"amount": 180, "time": "Today 10:20", "payer": "neha.v@okhdfc", "status": "SUCCESS", "debited": True, "refunded": False}}},
    "M1003": {"name": "Raju Chai Stall", "owner": "Raju", "city": "Lucknow", "phone": "9900112233", "monthly_volume": 45000, "has_loan": False, "soundbox": "SB-9102",
              "bank": {"acc": "12340098765432", "ifsc": "PUNB0123400", "changed_days_ago": 400, "verified": True},
              "settlements": {"ST-9203": {"amount": 1520, "date": "yesterday", "status": "SETTLED", "reason": "", "utr": "UTR7710CD12"}},
              "txns": {"430733401120": {"amount": 20, "time": "Today 08:02", "payer": "student21@ybl", "status": "FAILED", "debited": True, "refunded": False}},
              "coupons": {"CHAI5": {"code": "CHAI5", "percent": 5.0, "max_discount": 20.0, "valid_from": "2026-10-01", "valid_to": "2026-12-31", "status": "ACTIVE", "created_by": "merchant (app)",
                                    "redemptions": [{"customer": "Neha V", "bill": 120, "discount": 6.0, "paid": 114.0, "time": "Yesterday 17:40"},
                                                    {"customer": "Amit S", "bill": 60, "discount": 3.0, "paid": 57.0, "time": "Yesterday 18:05"},
                                                    {"customer": "Neha V", "bill": 80, "discount": 4.0, "paid": 76.0, "time": "Today 08:30"}]}}},
}

CUSTOMER_SEED = {
    "C2001": {"name": "Ankit Verma", "lang": "Hinglish", "city": "Delhi", "phone": "9811122233", "disputes": 0, "frozen": False,
              "txns": {"UPI-48821": {"amount": 420, "to": "Sharma General Store", "time": "Today 11:12", "status": "PENDING", "debited": True, "credited": False, "refunded": False}}},
    "C2002": {"name": "Lakshmi R", "lang": "Tamil", "city": "Chennai", "phone": "9840011223", "disputes": 1, "frozen": False, "txns": {},
              "emi": {"loan": "Paytm Personal Loan", "emi": 2150, "due": "Tomorrow", "late_fee": 250, "paid": False, "waived": False}},
    "C2003": {"name": "Rohan Mehta", "lang": "English", "city": "Pune", "phone": "9822334455", "disputes": 0, "frozen": False,
              "txns": {"UPI-99310": {"amount": 12000, "to": "quickpay8812@ybl", "time": "Today 02:14 AM", "status": "SUCCESS", "debited": True, "credited": True, "refunded": False}}},
}
BUYER_LANGS = ["Hinglish", "Hindi", "Tamil", "Marathi", "Bengali", "English"]

def buyers(mid, n=60):
    """Synthetic 90-day buyer book for a merchant (deterministic per merchant)."""
    import random as _r; rng = _r.Random(mid)
    return [{"vpa": f"cust{i:03d}@{rng.choice(['ybl', 'okaxis', 'paytm', 'okhdfc'])}", "lang": rng.choice(BUYER_LANGS),
             "visits_90d": rng.choice([1, 1, 2, 3, 4, 6, 9]), "last_visit_days": rng.randint(1, 120), "avg_ticket": rng.choice([120, 250, 400, 650, 900])} for i in range(n)]

def reset_world():
    global MERCHANTS, DEVICES, SESSIONS, APPROVALS, CACHE, MSGS
    MERCHANTS = copy.deepcopy(SEED); DEVICES = {"SB-7781": {"online": True, "fault": None, "last_ping": "just now"}, "SB-8820": {"online": True, "fault": None, "last_ping": "just now"},
               "SB-9102": {"online": True, "fault": None, "last_ping": "just now"}}
    SESSIONS, APPROVALS, CACHE, MSGS = {}, {}, {}, []
    global CUSTOMERS, CAMPAIGNS, ML_LOG, BANK_STATUS, HANDOFFS
    CUSTOMERS, CAMPAIGNS, ML_LOG, BANK_STATUS, HANDOFFS = copy.deepcopy(CUSTOMER_SEED), {}, [], "DEGRADED", []
reset_world()

def user(uid): return CUSTOMERS.get(uid) or MERCHANTS[uid]

def save_world():
    msgs = [{**{k: v for k, v in m.items() if k != "voice"}, **({"voice": {"source": "browser"}} if m.get("voice") else {})} for m in MSGS]
    with LOCK:
        db.execute("INSERT INTO kv VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", ("world", json.dumps({"m": MERCHANTS, "d": DEVICES, "c": CACHE, "msgs": msgs,
            "s": {k: {f: v for f, v in x.items() if f != "ctx"} for k, x in SESSIONS.items()}, "a": APPROVALS, "st": STAFF,
            "cu": CUSTOMERS, "ca": CAMPAIGNS, "ml": ML_LOG[-200:], "ho": HANDOFFS, "bank": BANK_STATUS}, ensure_ascii=False, default=str))); db.commit()

def load_world():
    global MERCHANTS, DEVICES, CACHE, MSGS, CUSTOMERS, CAMPAIGNS, ML_LOG, HANDOFFS, BANK_STATUS
    r = db.execute("SELECT v FROM kv WHERE k='world'").fetchone()
    if r:
        w = json.loads(r[0]); MERCHANTS, DEVICES, CACHE, MSGS = w["m"], w["d"], w["c"], w["msgs"]
        CUSTOMERS = w.get("cu", CUSTOMERS); CAMPAIGNS = w.get("ca", CAMPAIGNS); ML_LOG = w.get("ml", ML_LOG); HANDOFFS = w.get("ho", HANDOFFS); BANK_STATUS = w.get("bank", BANK_STATUS)
        SESSIONS.update({k: {**x, "ctx": lambda t, a, x=x: f"{x.get('query', '')} → {t} {a}"} for k, x in w.get("s", {}).items()}); APPROVALS.update(w.get("a", {})); STAFF.update(w.get("st", {}))

# ---------------- PII masking + immutable audit ----------------
def inr(n):
    """₹ in Indian grouping: 190000 -> ₹1,90,000."""
    s_ = str(int(n)); head, tail = s_[:-3], s_[-3:]
    return "₹" + (re.sub(r"(\d)(?=(\d\d)+$)", r"\1,", head) + "," + tail if head else tail)

def mask(s: str) -> str:
    s = re.sub(r"\b\d{9,18}\b", lambda m: "XXXX-" + m.group()[-4:], s)
    return re.sub(r"\b[A-Z]{4}0[A-Z0-9]{6}\b", lambda m: m.group()[:4] + "0XXXXXX", s)

def audit(s, phase, event, detail="", tokens=0):
    d = mask(detail if isinstance(detail, str) else json.dumps(detail, ensure_ascii=False))
    with LOCK:
        global LAST_HASH
        row = db.execute("SELECT hash FROM audit_chain ORDER BY id DESC LIMIT 1").fetchone(); LAST_HASH = row[0] if row else "GENESIS"  # DB is the source of truth
        ts = datetime.now().isoformat(timespec="milliseconds"); h = hashlib.sha256(f"{LAST_HASH}|{ts}|{s['id']}|{phase}|{event}|{d}".encode()).hexdigest()
        db.execute("INSERT INTO audit_chain(ts, ticket, phase, event, detail, prev, hash) VALUES(?,?,?,?,?,?,?)", (ts, s["id"], phase, event, d, LAST_HASH, h)); db.commit(); LAST_HASH = h
    s["phase"] = phase; s["tokens"] += tokens
    s["trace"].append({"phase": phase, "event": event, "detail": d, "ms": int((time.time() - s["opened"]) * 1000), "tokens": tokens})

# ---------------- Mock Paytm ecosystem ----------------
def find_txn(merchant_id, ref):
    ref = re.sub(r"\D", "", str(ref or ""))
    return next((k for k in MERCHANTS[merchant_id]["txns"] if ref and k.endswith(ref)), None)

def lookup_transaction(merchant_id, txn_id=None):
    k = find_txn(merchant_id, txn_id) if txn_id else None
    if txn_id and not k: return {"found": False, "searched": txn_id}
    return {x: dict(v) for x, v in MERCHANTS[merchant_id]["txns"].items() if not k or x == k}

def check_settlement_status(merchant_id):
    m = MERCHANTS[merchant_id]; b = m["bank"]
    return {"settlements": copy.deepcopy(m["settlements"]), "bank_recently_changed": b["changed_days_ago"] <= 3 and not b["verified"],
            "hold_reason": "BANK_CHANGE_HOLD" if b["changed_days_ago"] <= 3 and not b["verified"] else None, "bank_account": b["acc"], "bank_verified": b["verified"]}

def send_bank_update_link(merchant_id):
    MERCHANTS[merchant_id]["bank"]["verified"] = True  # demo: merchant completes penny-drop instantly
    return {"sent": True, "link": f"https://paytm.me/bank-verify/{uuid.uuid4().hex[:6]}", "to": MERCHANTS[merchant_id]["phone"]}

def trigger_resettlement(merchant_id, settlement_id, amount):
    st = MERCHANTS[merchant_id]["settlements"][settlement_id]
    if st["status"] == "SETTLED": return {"ok": False, "error": "IDEMPOTENT_BLOCK: already settled", "utr": st.get("utr")}
    if not MERCHANTS[merchant_id]["bank"]["verified"]: return {"ok": False, "error": "bank not verified"}
    st.update(status="SETTLED", utr="UTR" + uuid.uuid4().hex[:10].upper()); return {"ok": True, **st}

def process_refund(merchant_id, txn_id, amount):
    txn_id = find_txn(merchant_id, txn_id) or txn_id
    if txn_id not in MERCHANTS[merchant_id]["txns"]: return {"ok": False, "error": "txn not found"}
    if MERCHANTS[merchant_id]["txns"][txn_id]["refunded"]: return {"ok": False, "error": "IDEMPOTENT_BLOCK: already refunded"}
    MERCHANTS[merchant_id].setdefault("auto_refunded", 0); MERCHANTS[merchant_id]["auto_refunded"] += amount if amount < AUTO_LIMIT else 0
    MERCHANTS[merchant_id]["txns"][txn_id].update(refunded=True, status="REFUNDED"); return {"ok": True, "txn": txn_id, "amount": amount}

def ping_soundbox(merchant_id, device_id): return dict(DEVICES[device_id])

def restart_soundbox_iot(merchant_id, device_id):
    d = DEVICES[device_id]
    if d["fault"] == "network": d.update(online=True, fault=None, last_ping="just now")
    return {"device": device_id, "rebooted": True}

def update_bank_account(merchant_id, new_acc, ifsc):
    b = MERCHANTS[merchant_id]["bank"]; old = b["acc"]; b.update(acc=new_acc, ifsc=ifsc, changed_days_ago=0, verified=True)
    return {"ok": True, "old": old, "new": new_acc, "penny_drop": "SUCCESS", "name_match": "98%"}

def assign_senior_callback(merchant_id, ticket):
    return {"assigned": "senior-03 (Anil)", "eta_min": 10, "ticket": ticket}

def book_technician(merchant_id, slot="Tomorrow 11am-1pm"):
    return {"booked": True, "visit_id": "TV-" + uuid.uuid4().hex[:5].upper(), "slot": slot}

def check_loan_and_soundbox_eligibility(merchant_id):
    m = MERCHANTS[merchant_id]; v = m["monthly_volume"]
    if v >= 200000 and not m["has_loan"]:
        amt = min(500000, int(v * 0.4 // 10000 * 10000)) or 100000
        return {"product": "Business Loan", "pre_approved": amt, "emi_daily": int(amt * 1.16 / 365), "reason": f"GMV ₹{v:,}/mo, 0 bounces"}
    return {"product": "Soundbox Pro", "offer": "₹1 activation + 4G", "reason": "high UPI share"}

TOOLS = {f.__name__: f for f in [lookup_transaction, check_settlement_status, send_bank_update_link, trigger_resettlement, process_refund, update_bank_account, assign_senior_callback,
                                  ping_soundbox, restart_soundbox_iot, book_technician, check_loan_and_soundbox_eligibility]}

# ---------------- Guardrails ----------------
def risk_score(mid, tool, args):
    if mid in CUSTOMERS: return (SESSIONS_ML.get(mid, {}).get("risk_score", 80), ["ML fraud model", "customer funds at risk"])
    m = MERCHANTS[mid]; r, why = 10, []
    if m["bank"]["changed_days_ago"] <= 1: r += 35; why.append("bank changed <24h")
    if args.get("amount", 0) >= 10000: r += 20; why.append("high value")
    if m["bank"]["verified"]: r -= 15; why.append("penny-drop verified")
    if m["monthly_volume"] >= 200000: r -= 5; why.append("trusted GMV history")
    return max(0, min(100, r)), why

def tier(tool, args, mid="M1001"):
    if tool == "trigger_resettlement": return "NEEDS_APPROVAL", "Re-settlement moves merchant money to a newly changed bank account"
    if tool == "book_technician": return "NEEDS_APPROVAL", "Field visit has cost"
    if tool == "update_bank_account": return "NEEDS_APPROVAL", "Bank/KYC override — payout destination change"
    if tool == "assign_senior_callback": return "NEEDS_APPROVAL", "Negative CSAT — senior must own the callback"
    if tool == "process_refund" and args.get("amount", 0) >= AUTO_LIMIT: return "NEEDS_APPROVAL", f"Refund ≥ ₹{AUTO_LIMIT}"
    if tool == "process_refund" and MERCHANTS[mid].get("auto_refunded", 0) + args.get("amount", 0) > DAILY_AUTO_CAP: return "NEEDS_APPROVAL", f"Daily auto-refund cap ₹{DAILY_AUTO_CAP} reached"
    return "AUTO", "Safe / reversible / below ₹500"

# ---------------- SOP RAG (BM25-lite) ----------------
SOPS = [
    ("SOP-SETTLE-04", "settlement", "settlement nahi aaya paisa account credit delay payout bank t+1 settle kal", "Check settlement status. If BANK_CHANGE_HOLD, send bank update link, then re-settle after human approval. Verify UTR."),
    ("HELP-SETTLE-TAT", "settlement", "settlement kab aayega time cycle t+1 instant settlement holiday sunday", "Standard T+1 by 11am; holidays shift to next working day. Instant settlement for eligible merchants."),
    ("SOP-IOT-11", "soundbox", "soundbox bol nahi raha silent awaaz band speaker device offline beep", "Ping device, remote reboot via IoT, re-ping. If still offline, book technician (needs approval)."),
    ("HELP-SB-POWER", "soundbox", "soundbox charge battery band on nahi ho raha light nahi jal rahi", "Charge 2h with original adapter; if no LED, book technician (needs approval)."),
    ("SOP-PAY-07", "failed_payment", "payment fail customer paisa kat gaya debit failed transaction refund", "Look up txn. If debited & failed, refund. <₹500 auto, ≥₹500 needs approval. Verify refund."),
    ("HELP-REFUND-TAT", "failed_payment", "refund kab milega customer ko kitne din reversal status", "Auto-reversal within 48h; bank credit 3-5 working days. Share RRN with customer."),
    ("SOP-KYC-02", "kyc_bank", "kyc bank account change update badalna ifsc details document naya account", "Send secure link → OTP → penny-drop + name match → bank override needs human approval → verify."),
    ("HELP-QR", "other", "qr code scan nahi ho raha print naya qr sticker", "Send new QR via app; dispatch printed QR standee in 3 days."),
    ("SOP-SALES-01", "sales", "loan karza paisa chahiye business badhana naya soundbox offer", "Eligibility check on GMV. ≥₹2L/mo & no loan → pre-approved Business Loan; else Soundbox Pro."),
]
_tok = lambda s: re.findall(r"[a-z0-9₹+]+", s.lower())
def _grams(s): s = f" {s.lower()} "; return {s[i:i + 3] for i in range(len(s) - 2)}
def rag(q):
    """Hybrid retrieval: BM25-lite term score + char-trigram cosine (robust to Hinglish spellings like nai/nahi, setelment)."""
    qt, qg, N = set(_tok(q)), _grams(q), len(SOPS)
    def sc(d):
        lex = sum(math.log(1 + N / (1 + sum(t in _tok(x[2]) for x in SOPS))) for t in qt if t in _tok(d[2] + " " + d[1]))
        dg = _grams(d[2]); return round(lex + 4 * len(qg & dg) / math.sqrt(len(qg) * len(dg) or 1), 2)
    best = max(SOPS, key=sc)
    return {"id": best[0], "intent": best[1], "steps": best[3], "score": sc(best), "method": "hybrid BM25 + trigram"} if sc(best) > 0.5 else None

# ---------------- Sarvam AI layer ----------------
def _h(): return {"api-subscription-key": SARVAM_KEY}

def sarvam_llm(system, user):
    if not SARVAM_KEY: return None, 0
    try:
        r = httpx.post(f"{SARVAM}/v1/chat/completions", headers={**_h(), "Authorization": f"Bearer {SARVAM_KEY}"}, timeout=12,
                       json={"model": "sarvam-105b-conversations", "temperature": 0.2, "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]}).json()
        return re.sub(r"<think>.*?</think>", "", r["choices"][0]["message"]["content"], flags=re.S).strip(), r.get("usage", {}).get("total_tokens", 0)
    except Exception: return None, 0

def stt(audio: bytes, fname="note.webm"):
    if not SARVAM_KEY: return None
    try:
        return httpx.post(f"{SARVAM}/speech-to-text", headers=_h(), timeout=20, files={"file": (fname, audio)},
                          data={"model": "saarika:v2.5", "language_code": "unknown"}).json().get("transcript")
    except Exception: return None

def detect_lang(text):
    if re.search(r'[ऀ-ॿ]', text): return "hi-IN", "anushka"
    if re.search(r'[஀-௿]', text): return "ta-IN", "saranya"
    if re.search(r'[ఀ-౿]', text): return "te-IN", "amartya"
    if re.search(r'[ঀ-৿]', text): return "bn-IN", "priti"
    if re.search(r'[઀-૿]', text): return "gu-IN", "anushka"
    if re.search(r'[ಀ-೿]', text): return "kn-IN", "arvind"
    if re.search(r'[ഀ-ൿ]', text): return "ml-IN", "arvind"
    words = set(re.findall(r'[a-zA-Z]+', text.lower()))
    hi_cues = {"aapka", "aap", "nahi", "aaya", "karo", "hai", "hain", "ke", "liye", "mein", "gaya", "ho", "kar", "bheja", "tha", "se", "raha"}
    if words & hi_cues: return "hi-IN", "anushka"
    return "en-IN", "meera"

def tts(text):
    lang, spk = detect_lang(text)
    if SARVAM_KEY:
        try:
            r = httpx.post(f"{SARVAM}/text-to-speech", headers=_h(), timeout=20, json={"inputs": [text[:500]], "target_language_code": lang, "speaker": "priya", "model": "bulbul:v3"})
            return {"audio": r.json()["audios"][0], "mime": "audio/wav", "source": "bulbul", "lang": lang}
        except Exception: pass
    if edge_tts:
        try:
            edge_voice = "hi-IN-SwaraNeural" if lang == "hi-IN" else "en-IN-NeerjaNeural"
            async def _gen():
                b = b""
                async for c in edge_tts.Communicate(re.sub(r"[^\w\s.,?!₹-]", "", text)[:600], edge_voice).stream():
                    if c["type"] == "audio": b += c["data"]
                return b
            audio = ThreadPoolExecutor(1).submit(asyncio.run, _gen()).result(timeout=25)  # works inside async endpoints too
            if audio: return {"audio": base64.b64encode(audio).decode(), "mime": "audio/mpeg", "source": "neural", "lang": lang}
        except Exception: pass
    return {"source": "browser", "lang": lang}

# ---------------- Understand / phrase ----------------
INTENTS = {"kyc_bank": r"kyc|bank.{0,15}(badal|change|update)|ifsc|account change|बैंक",
           "settlement": r"settle|payout|paisa nahi aaya|credit nahi|settlement delay|not credited|money not received|पैसा|सेटलमेंट",
           "soundbox": r"sound ?box|bol nahi|silent|awaaz|not speaking|no sound|speaker|offline|device silent|mute|आवाज़|साउंडबॉक्स",
           "failed_payment": r"fail|kat gaya|kata|debit|refund|nahi mila|account mein nahi|failed payment|money deducted|deducted but failed|कट गया|रिफंड",
           "sales": r"loan|karza|offer|naya soundbox|new soundbox|business loan|लोन|finance"}
def understand(text):
    out, tk = sarvam_llm("Classify the Paytm merchant query into one of: settlement, soundbox, failed_payment, kyc_bank, sales, other. One word only.", text)
    w = (out or "").split()[0].strip(".").lower() if out else ""
    if w in list(INTENTS) + ["other"]: return w, "sarvam-m", tk
    for k, p in INTENTS.items():
        if re.search(p, text, re.I): return k, "sarvam-m (sim)", 180
    return "other", "unclear", 180

def phrase(s, draft, facts):
    out, tk = sarvam_llm("You are Saathi, Paytm's merchant teammate. Rewrite in the same language as the merchant (English, Hinglish, or Hindi/regional), under 35 words. Keep every number/ID exact. No preamble.",
                         f"Facts: {json.dumps(facts, ensure_ascii=False)}\nDraft: {draft}")
    s["tokens"] += tk or 90
    return out if out and len(out) < 400 else draft

# ---------------- Agent loop ----------------
def call(s, tool, **args):
    t, why = tier(tool, args, s["merchant"])
    if t == "NEEDS_APPROVAL":
        dup = next((a for a in APPROVALS.values() if a["status"] == "PENDING" and a["tool"] == tool and a["args"] == args), None)
        if dup: s["status"] = "WAITING_APPROVAL"; audit(s, "HUMAN", f"↺ {tool} already awaiting approval ({dup['id']}) — not duplicating", args); return None
        aid = "AP-" + uuid.uuid4().hex[:5].upper()
        before = snapshot(s["merchant"])
        APPROVALS[aid] = {"id": aid, "ticket": s["id"], "tool": tool, "args": args, "status": "PENDING", "risk": why, "before": before, "score": risk_score(s["merchant"], tool, args),
                          "summary": s["ctx"](tool, args), "created": time.time(), "ml": s.get("ml"),
                          "actions": [t["event"] for t in s["trace"] if t["phase"] == "ACT"][-4:]}
        s["status"] = "WAITING_APPROVAL"; s["auto"] = False
        audit(s, "HUMAN", f"⏸ {tool} → NEEDS_APPROVAL ({aid})", {"tool": tool, "args": args, "tier": t, "reason": why})
        amt = f" ₹{args['amount']:,}" if args.get("amount") else ""
        staff_notify(f"Approval needed *{aid}* · {tool}{amt}\n{APPROVALS[aid]['summary'][:140]}\nReply *approve {aid} <PIN>* ya *reject {aid}*", aid); return None
    res = TOOLS[tool](s["merchant"], **args); s["tools"] += 1
    audit(s, "ACT", f"✔ {tool} [AUTO]", {"tool": tool, "args": args, "tier": t, "result": res}); return res

def snapshot(mid):
    if mid in CUSTOMERS:
        c = CUSTOMERS[mid]
        return {"frozen": c["frozen"], "txns": {k: v["status"] for k, v in c["txns"].items()}, **({"emi_paid": c["emi"]["paid"], "late_fee_waived": c["emi"]["waived"]} if c.get("emi") else {})}
    m = MERCHANTS[mid]
    return {"settlements": {k: v["status"] for k, v in m["settlements"].items()}, "bank_verified": m["bank"]["verified"],
            "soundbox_online": DEVICES[m["soundbox"]]["online"], "txns": {k: v["status"] for k, v in m["txns"].items()}}

def reply(s, text, facts=None, voice=False, cta=None, kind=None, options=None):
    text = phrase(s, text, facts) if facts is not None else text
    msg = {"from": "saathi", "text": text, "ts": time.time(), "ticket": s["id"], "merchant": s["merchant"]}
    if voice: msg["voice"] = tts(text)
    if cta: msg["cta"] = cta
    if kind: msg["kind"] = kind
    if options: msg["options"] = options
    MSGS.append(msg); return msg

def resolve(s, note, cache_key=None, plan=None):
    s["status"] = "VERIFIED"; s["resolved"] = time.time()
    if cache_key and plan and not s["cache_hit"]: CACHE[cache_key] = {"plan": plan, "hits": 0, "learned_from": s["id"]}
    cost = round(s["tokens"] / 1000 * INR_PER_1K_TOKENS + s["tools"] * 0.05 + (0 if s["auto"] else HUMAN_MIN_COST), 2)
    audit(s, "VERIFY", "✅ Verified — follow-up scheduled in 24h", {"note": note, "tokens": s["tokens"], "cost_inr": cost})
    with LOCK:
        db.execute("UPDATE tix SET status='VERIFIED', auto=?, cache=?, tokens=?, cost=?, resolved=? WHERE id=?",
                   (int(s["auto"]), int(s["cache_hit"]), s["tokens"], cost, s["resolved"], s["id"])); db.commit()
    if s["merchant"] in MERCHANTS and s["intent"] != "sales" and not MERCHANTS[s["merchant"]].get("pitched") and calm(s): MERCHANTS[s["merchant"]]["pitched"] = True; pivot(s)

def calm(s):
    """Pitch only at a quiet moment: no other open ticket, pending approval or unanswered question for this merchant."""
    busy = [x for x in SESSIONS.values() if x is not s and x["merchant"] == s["merchant"] and x["status"] in ("OPEN", "DIAGNOSING", "WAITING_APPROVAL", "EXECUTING", "NEEDS_INFO")]
    return not busy and not any(a["status"] == "PENDING" for a in APPROVALS.values())

def pivot(s):
    """Support-to-Sales: pitch at the high-CSAT moment right after a verified fix."""
    o = TOOLS["check_loan_and_soundbox_eligibility"](s["merchant"])
    audit(s, "DIAGNOSE", f"💰 Sales pivot: {o['product']}", o)
    if o["product"] == "Business Loan":
        reply(s, f"Waise, aapke store ki badhti bikri dekhte hue aap {inr(o['pre_approved'])} ke Paytm Business Loan ke liye pre-approved hain, sirf ₹{o['emi_daily']}/din. 1-click mein apply kar doon?",
              cta={"label": "✅ Haan, apply karo", "product": o["product"], "amount": o["pre_approved"]}, kind="pitch")
    else:
        reply(s, f"Waise, Soundbox Pro {o['offer']} pe mil raha hai — ₹1 aapke agle settlement se adjust hoga. Upgrade kar doon?", cta={"label": "✅ Haan, ₹1 settlement se kaat ke upgrade karo", "product": o["product"], "amount": 0}, kind="pitch")

def signature(intent, ctx): return f"{intent}:{ctx}"

def run_agent(s, text, proactive=False, live=True):
    if "ml" not in s:
        if ml_route(s, text, proactive): s["ml"]["acted"] = True; return
        s["ml"]["acted"] = False  # ML was unsure / not its case: the agent's own intent is what counts
    if s["merchant"] in CUSTOMERS: return handoff(s, "Unclassified customer query", text)
    if live and LLM_KEY: return llm_agent(s, text, proactive)
    m = MERCHANTS[s["merchant"]]; s["status"] = "DIAGNOSING"
    pre = next((k for k, p in INTENTS.items() if re.search(p, text, re.I)), None)
    hint = {"settlement": "BANK_CHANGE_HOLD" if check_settlement_status(s["merchant"])["bank_recently_changed"] else "OTHER",
            "soundbox": DEVICES[m["soundbox"]].get("fault") or "OK"}.get(pre, "")
    key = signature(pre, hint)
    if key in CACHE:
        s["cache_hit"] = True; CACHE[key]["hits"] += 1; intent = pre
        audit(s, "UNDERSTAND", f"⚡ CACHE_HIT {key} — skipping LLM diagnosis", {"learned_from": CACHE[key]["learned_from"], "plan": CACHE[key]["plan"], "tokens_saved": "~80%"}, tokens=40)
        s["sop"] = {"id": "VERIFIED-FIX-CACHE", "steps": " → ".join(CACHE[key]["plan"]), "score": 1.0}
    else:
        intent, how, tk = understand(text)
        audit(s, "UNDERSTAND", f"Intent = {intent} ({how})", {"merchant": s["merchant"], "query": text, "proactive": proactive, "ctx": snapshot(s["merchant"])}, tokens=tk)
        s["sop"] = rag(text + " " + intent)
        audit(s, "DIAGNOSE", f"SOP matched: {s['sop']['id'] if s['sop'] else 'none'}", s["sop"] or "Escalate", tokens=420)
    s["intent"] = intent
    if intent != "other": m["unclear"] = 0
    with LOCK:
        db.execute("INSERT INTO tix VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET intent=excluded.intent, cache=excluded.cache", (s["id"], s["merchant"], intent, "OPEN", 1, int(s["cache_hit"]), 0, 0, s["opened"], None)); db.commit()

    if intent == "kyc_bank":
        link = call(s, "send_bank_update_link")
        reply(s, f"Bank badalne ke liye secure link {mask(link['to'])} pe bheja hai. Penny-drop verify hote hi naya account activate karne ke liye approval le raha hoon 🔐")
        s["ctx"] = lambda tool, a: f"{m['name']}: merchant requested bank change to {mask(a['new_acc'])} ({a['ifsc']}). Link sent & OTP done; activation needs approval."
        call(s, "update_bank_account", new_acc="60200987654321", ifsc="ICIC0004567"); return
    if intent == "settlement":
        st = call(s, "check_settlement_status")
        failed = {k: v for k, v in st["settlements"].items() if v["status"] != "SETTLED"}
        if not failed:
            reply(s, "Aapke saare settlements credit ho chuke hain ✅ Kuch aur madad chahiye?", st); return resolve(s, "No pending settlement")
        if st["bank_recently_changed"]:
            audit(s, "DIAGNOSE", "Root cause: BANK_CHANGE_HOLD (account changed 1 day ago, unverified)", "→ send link + request re-settlement")
            link = call(s, "send_bank_update_link")
            amt = sum(v["amount"] for v in failed.values())
            lead = f"Namaste {m['owner'].split()[0]} ji, aapke shikayat karne se pehle hi bata doon — " if proactive else "Samajh gaya 🙏 "
            reply(s, f"{lead}aapka ₹{amt:,} ka settlement bank change ki wajah se ruka tha. Verification link {mask(link['to'])} pe bhej diya hai, re-settlement ke liye approval le raha hoon.", voice=proactive)
        for sid, v in failed.items():
            s["ctx"] = lambda tool, a, v=v: f"{m['name']}: settlement {a['settlement_id']} ₹{a['amount']:,} failed ({v['reason']}). Bank changed {m['bank']['changed_days_ago']}d ago → link sent, new account now penny-drop verified. Merchant already informed."
            s["plan"] = ["check_settlement_status", "send_bank_update_link", "trigger_resettlement[approval]", "verify UTR"]
            call(s, "trigger_resettlement", settlement_id=sid, amount=v["amount"])
        return
    if intent == "soundbox":
        dev = m["soundbox"]; before = call(s, "ping_soundbox", device_id=dev)
        if proactive: reply(s, f"Namaste {m['owner'].split()[0]} ji, aapka Soundbox {dev} {before['last_ping']} se offline dikh raha hai. Main abhi remotely theek kar raha hoon 🔧")
        call(s, "restart_soundbox_iot", device_id=dev)
        after = call(s, "ping_soundbox", device_id=dev)
        s["diff"] = {"before": before, "after": after}; audit(s, "VERIFY", f"Re-ping: online {before['online']} → {after['online']}", s["diff"])
        if after["online"]:
            reply(s, f"Aapka Soundbox {dev} remotely restart kar diya, ab online hai ✅ Ek test payment karke check kar lijiye.", after, voice=True)
            return resolve(s, "Device online", key, ["ping_soundbox", "restart_soundbox_iot", "re-ping verify"])
        s["ctx"] = lambda tool, a: f"{m['name']}: Soundbox {dev} still offline after remote reboot (fault={after['fault']}). Technician visit requested."
        call(s, "book_technician", slot="Tomorrow 11am-1pm")
        reply(s, "Remote restart se theek nahi hua. Technician visit ke liye approval le raha hoon 🙏"); return
    if intent == "failed_payment":
        tx = call(s, "lookup_transaction")
        s["ctx"] = lambda tool, a: f"{m['name']}: txn {a['txn_id']} ₹{a['amount']:,} debited but failed. Refund ≥ ₹{AUTO_LIMIT} needs approval."
        cands = {k: t for k, t in tx.items() if t["debited"] and t["status"] == "FAILED" and not t["refunded"]}
        nums = set(re.findall(r"\d+", text.replace(",", "")))
        picked = {k: t for k, t in cands.items() if k in text or k[-4:] in nums or str(t["amount"]) in nums}
        if not cands:
            reply(s, "Maine aapke recent transactions check kiye — koi failed ya atka hua payment nahi mila ✅ Customer se UPI ref number mangwa lijiye, main turant check karunga."); return resolve(s, "No failed txn")
        if not picked and len(cands) > 1:
            s["status"] = "NEEDS_INFO"; audit(s, "DIAGNOSE", f"{len(cands)} failed txns found → asking merchant which one (no refund yet)", cands)
            reply(s, "Mujhe ye failed payments mile. Kaunsa wala?", options=[f"{t['time'].replace('Today ', '')} wala ₹{t['amount']:,} ({t['payer'][:3]}…) — ref ...{k[-4:]} refund karo" for k, t in cands.items()]); return
        pending = False
        for tid, t in (picked or cands).items():
            if True:
                if call(s, "process_refund", txn_id=tid, amount=t["amount"]):
                    chk = call(s, "lookup_transaction", txn_id=tid)[tid]; s["diff"] = {"before": t, "after": chk}
                    audit(s, "VERIFY", f"{tid}: {t['status']} → {chk['status']}", s["diff"])
                    reply(s, f"Txn {tid} ka ₹{t['amount']} refund ho gaya aur verify kar liya ✅", chk)
                else: pending = True
        if pending: reply(s, "Bade amount wale refund ke liye senior se approval le raha hoon, jaldi update dunga 🙏")
        else: resolve(s, "Refunds verified")
        return
    if intent == "sales":
        o = call(s, "check_loan_and_soundbox_eligibility")
        reply(s, f"Aapka business ₹{m['monthly_volume']:,}/mahina hai 💪 ₹{o.get('pre_approved', 0):,} ka pre-approved Business Loan ready hai. Apply karein?" if o["product"] == "Business Loan" else "Soundbox Pro ₹1 activation pe! Lagwa doon?",
              o, voice=True, cta={"label": "✅ Haan, apply karo", "product": o["product"], "amount": o.get("pre_approved", 0)}, kind="pitch")
        return resolve(s, "Offer pitched")
    m["unclear"] = m.get("unclear", 0) + 1
    if m["unclear"] == 1:  # low confidence -> clarify instead of acting on a guess
        s["status"] = "NEEDS_INFO"
        audit(s, "DIAGNOSE", "Low confidence → asking merchant to clarify (no tool called)", text)
        reply(s, "Main samajhna chahta hoon taaki sahi kaam karun 🙏 Inmein se kya hua hai?",
              options=["Customer ne pay kiya par mere account mein nahi aaya", "Customer ka paisa kata par payment fail hua", "Kal ka settlement nahi aaya", "Soundbox ne announce nahi kiya"])
        return
    m["unclear"] = 0; s["status"] = "ESCALATED"; s["auto"] = False
    audit(s, "HUMAN", "Still unclear → escalated with full context summary", text)
    reply(s, "Ye case main human expert ko poori summary ke saath de raha hoon, aapko dobara kuch batana nahi padega 🙏")

def resume(ap):
    s = SESSIONS[ap["ticket"]]; tool, a = ap["tool"], ap["args"]
    if tool in ML_TOOLS: return ml_resume(s, ap)
    audit(s, "HUMAN", f"👤 Approved by {APPROVER} · PIN ✓ · risk {ap['score'][0]}/100 → executing {tool}", a)
    res = TOOLS[tool](s["merchant"], **a); s["tools"] += 1; audit(s, "ACT", f"✔ {tool}", {"tool": tool, "args": a, "result": res})
    s["diff"] = {"before": ap["before"], "after": snapshot(s["merchant"])}
    if s.get("llm"): return llm_resume(s, ap, res)
    if tool == "trigger_resettlement":
        v = check_settlement_status(s["merchant"])["settlements"][a["settlement_id"]]
        audit(s, "VERIFY", f"Re-query {a['settlement_id']}: FAILED → {v['status']}", s["diff"])
        if v["status"] == "SETTLED":
            reply(s, f"Khushkhabri! ₹{a['amount']:,} ka settlement aapke naye bank account mein bhej diya, UTR {v['utr']}. Verify kar liya ✅", {"amount": a["amount"], "utr": v["utr"]}, voice=True)
            resolve(s, f"UTR {v['utr']}", signature("settlement", "BANK_CHANGE_HOLD"), s.get("plan"))
        else: reply(s, "Re-settlement abhi pending hai, main 30 min mein dobara check karunga.")
    elif tool == "process_refund":
        reply(s, f"Txn {a['txn_id']} ka ₹{a['amount']:,} refund approve hokar process ho gaya ✅", a, voice=True); resolve(s, "Refund verified")
    elif tool == "update_bank_account":
        b = MERCHANTS[s["merchant"]]["bank"]; audit(s, "VERIFY", f"Bank now {mask(b['acc'])} · penny-drop {res['penny_drop']} · name match {res['name_match']}", s["diff"])
        reply(s, f"Aapka naya bank account {mask(b['acc'])} verify hokar active ho gaya ✅ Agla settlement isi mein aayega.", {"acc": mask(b["acc"])}, voice=True); resolve(s, "Bank updated")
    elif tool == "assign_senior_callback":
        reply(s, f"{res['assigned']} aapko {res['eta_min']} min mein call karenge. Asuvidha ke liye maafi 🙏"); s["status"] = "ESCALATED"
    elif tool == "book_technician":
        reply(s, f"Technician visit book ho gaya ✅ {res['slot']}, ID {res['visit_id']}.", res, voice=True); resolve(s, res["visit_id"])

# ---------------- Live LLM agent (real tool-calling; guardrails still enforced by call()) ----------------
TOOL_SPECS = {
    "lookup_transaction": ("Look up the merchant's recent transactions (optionally one txn_id).", {"txn_id": "string"}, []),
    "check_settlement_status": ("Get settlements and whether a recent bank change is holding payouts.", {}, []),
    "send_bank_update_link": ("Send the merchant a secure bank-verification link.", {}, []),
    "trigger_resettlement": ("Re-send a failed settlement. Needs human approval.", {"settlement_id": "string", "amount": "integer"}, ["settlement_id", "amount"]),
    "process_refund": ("Refund a debited-but-failed txn. >=500 needs human approval.", {"txn_id": "string", "amount": "integer"}, ["txn_id", "amount"]),
    "ping_soundbox": ("Check if a Soundbox device is online.", {"device_id": "string"}, ["device_id"]),
    "restart_soundbox_iot": ("Remotely reboot a Soundbox.", {"device_id": "string"}, ["device_id"]),
    "book_technician": ("Book a field technician visit. Needs human approval.", {"slot": "string"}, []),
    "check_loan_and_soundbox_eligibility": ("Check pre-approved loan / Soundbox offer eligibility.", {}, []),
    "update_bank_account": ("Change the payout bank account. Needs human approval.", {"new_acc": "string", "ifsc": "string"}, ["new_acc", "ifsc"]),
}
LLM_TOOLS = [{"type": "function", "function": {"name": n, "description": d, "parameters": {"type": "object", "properties": {k: {"type": t} for k, t in ps.items()}, "required": req}}}
             for n, (d, ps, req) in TOOL_SPECS.items()]
READ_ONLY = {"lookup_transaction", "check_settlement_status", "ping_soundbox", "check_loan_and_soundbox_eligibility"}

def llm_system(s, sop, cached):
    m = MERCHANTS[s["merchant"]]
    return f"""You are Saathi, Paytm's autonomous support & sales teammate for merchants. Close tickets with verified tool actions, not FAQ answers.
Loop: UNDERSTAND the query -> DIAGNOSE with the SOP -> ACT with tools -> VERIFY by re-querying state after any change -> then reply.
Rules:
- Never guess. If the merchant's problem is unclear, ask ONE short clarifying question and call no tools.
- "Customer left without paying / paisa diye bina chala gaya / didn't pay me" is NOT a refund case: the merchant lost money, the customer was not charged. Check recent payments around that time, say whether any attempt came in, and offer a Paytm payment link the merchant can share. Never refund a customer who did not pay. But FAILED + customer_debited=true means the customer WAS charged and is owed a refund — that is a refund case.
- If the merchant says all/both/saare/dono, act on all matching payments without asking again.
- If a tool can do it (create_coupon, refunds, links…), do it yourself and confirm — never tell the merchant to do it in the app.
- NEVER ask the merchant for data you can fetch. Merchants don't know txn IDs: call lookup_transaction first, then identify the payment by amount/time/payer (e.g. "10:32 wala ₹349, rahul.k@… se?").
- If the merchant gives an ID/number that doesn't match, say it wasn't found and list the matching recent payments (time, amount, masked payer, last 4 of ref).
- If several transactions could match, ask which one; act only on the one the merchant means.
- Some tools need human approval; if a tool returns PENDING_HUMAN_APPROVAL, tell the merchant it is with a senior for approval. Never claim it is done.
- A message starting with EVENT is a Paytm backend alert: message the merchant first, proactively, by name.
- Do not pitch loans/offers unless the merchant asks.
- Address the merchant by first name + "ji" only in the first reply of a conversation, not every message.
- Multi-language support: judge the language ONLY from the merchant's LATEST message (ignore earlier turns). Reply in the SAME language and script as that message's query. If English is used, reply in concise natural English. If Hinglish (Roman script) is used, reply in warm Hinglish. If pure Hindi (Devanagari) or another Indic language is used, reply in that language. Under 40 words, stating exactly what you did and verified. Mask account/phone numbers as XXXX-1234.
Merchant: {m['name']} (owner {m['owner']}, {m.get('city', '')}), id {s['merchant']}, Soundbox {m['soundbox']}, GMV Rs {m['monthly_volume']}/month.
Account context (live): recent payments {json.dumps([{"ref": k, "time": v["time"], "amount": v["amount"], "payer": v["payer"][:3] + "…", "status": v["status"], "customer_debited": v["debited"], "refunded": v["refunded"]} for k, v in m["txns"].items()], ensure_ascii=False)};
settlements {json.dumps({k: {"amount": v["amount"], "status": v["status"]} for k, v in m["settlements"].items()})}; Soundbox online={DEVICES[m["soundbox"]]["online"]}.
Mention refs to the merchant only as last 4 digits. Use this context to propose the likely payment right away (e.g. "10:32 wala ₹349?") instead of asking open questions; still call tools to act and to verify.
Retrieved SOP: {json.dumps(sop, ensure_ascii=False) if sop else 'none'}
{('Verified fix from a past ticket (reuse it, skip re-diagnosis): ' + ' -> '.join(cached['plan'])) if cached else ''}"""

def llm_chat(msgs, tools=True):
    """Try providers in order; a 429/quota/5xx/timeout puts that provider on cooldown and falls through to the next."""
    global LLM_PROVIDER, LLM_MODEL
    errs = []
    for p in LLM_CHAIN + LLM_CHAIN:  # second pass = one retry per provider (transient 400/503)
        if p["cool_until"] > time.time(): continue
        try:
            r = httpx.post(f"{p['base']}/chat/completions", headers={"Authorization": f"Bearer {p['key']}", **({"api-subscription-key": p["key"]} if p["name"] == "sarvam" else {})}, timeout=40,
                           json={"model": p["model"], "messages": msgs, **({"tools": LLM_TOOLS} if tools else {}), "temperature": 0.2})
            if r.status_code == 200:
                LLM_PROVIDER, LLM_MODEL = p["name"], p["model"]; j = r.json()
                return j["choices"][0]["message"], j.get("usage", {}).get("total_tokens", 0), errs
            errs.append(f"{p['name']} {r.status_code}: {r.text[:120]}")
            if r.status_code in (401, 402, 403, 429): p["cool_until"] = time.time() + 120
        except Exception as e:
            errs.append(f"{p['name']}: {e}"); p["cool_until"] = time.time() + 30
    raise RuntimeError(" | ".join(errs) or "all LLM providers cooling down")

def llm_loop(s, acted=False):
    for _ in range(8):
        msg, tk, errs = llm_chat(s["llm_msgs"]); s["tokens"] += tk
        if errs: audit(s, "HUMAN", f"↪ Provider fallback → now on {LLM_PROVIDER}/{LLM_MODEL}", errs)
        calls = msg.get("tool_calls") or []
        s["llm_msgs"].append({"role": "assistant", "content": msg.get("content") or "", **({"tool_calls": calls} if calls else {})})
        if not calls:
            text = (msg.get("content") or "").strip() or "Ek minute, main check kar raha hoon 🙏"
            reply(s, re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip())
            break
        audit(s, "DIAGNOSE", f"🧠 {LLM_MODEL} chose: " + ", ".join(c["function"]["name"] for c in calls), [c["function"] for c in calls], tokens=tk)
        for c in calls:
            name = c["function"]["name"]
            try:
                args = json.loads(c["function"].get("arguments") or "{}")
                if name not in TOOLS: raise ValueError("unknown tool")
                if acted and name in READ_ONLY: audit(s, "VERIFY", f"🔎 Re-checking state via {name}", args)
                if name == "process_refund" and not merchant_picked(s, args.get("txn_id")):
                    out = {"error": "BLOCKED: several failed payments match. Ask the merchant which one (time + amount) before refunding."}
                    audit(s, "DIAGNOSE", "🛑 Guardrail: refund blocked until merchant confirms which payment", args)
                    s["llm_msgs"].append({"role": "tool", "tool_call_id": c.get("id", name), "content": json.dumps(out)}); continue
                res = call(s, name, **args)
                out = {"status": "PENDING_HUMAN_APPROVAL"} if res is None else res
                if res is not None and name not in READ_ONLY: acted = True
            except Exception as e:
                out = {"error": str(e)}; audit(s, "ACT", f"⚠ {name} failed: {e}", c["function"])
            s["llm_msgs"].append({"role": "tool", "tool_call_id": c.get("id", name), "content": json.dumps(out, ensure_ascii=False, default=str)})
    pending = any(a["ticket"] == s["id"] and a["status"] == "PENDING" for a in APPROVALS.values())
    if pending: s["status"] = "WAITING_APPROVAL"; return
    if not acted: s["status"] = "NEEDS_INFO"; return
    ok, why = healthy(s)
    if ok: return resolve(s, "Verified by live agent")
    s["nudges"] = s.get("nudges", 0) + 1
    audit(s, "VERIFY", f"❌ Verification failed: {why} → agent must continue", snapshot(s["merchant"]))
    if s["nudges"] > 2: s["status"] = "ESCALATED"; return reply(s, "Ye case main senior ko poori summary ke saath de raha hoon 🙏")
    s["llm_msgs"].append({"role": "user", "content": f"[SYSTEM] Not resolved yet: {why}. Continue the SOP (use the approval-gated tool if needed). Do not tell the merchant it is fixed until verified."})
    return llm_loop(s, acted)

def merchant_picked(s, ref):
    """Refund only what the merchant identified: unique candidate, or their own words name its amount/time/ref."""
    mid = s["merchant"]; k = find_txn(mid, ref)
    cands = [x for x, t in MERCHANTS[mid]["txns"].items() if t["debited"] and t["status"] == "FAILED" and not t["refunded"]]
    if not k or len(cands) <= 1: return True
    t = MERCHANTS[mid]["txns"][k]; said = " ".join(x["text"] for x in MSGS if x.get("merchant") == mid and x["from"] == "merchant")[-400:].replace(",", "")
    if re.search(r"\b(saare|sare|sab|sabhi|dono|all|both)\b", said[-120:], re.I): return True  # merchant chose all of them
    return str(t["amount"]) in said or t["time"].split()[-1] in said or k[-4:] in said or k[-3:] in said

def healthy(s):
    """Ground-truth check of the world state, independent of what the LLM claims."""
    snap = snapshot(s["merchant"])
    bad = [k for k, v in snap["settlements"].items() if v != "SETTLED"]
    if s["intent"] == "settlement" and bad: return False, f"settlement {', '.join(bad)} still FAILED"
    if s["intent"] == "soundbox" and not snap["soundbox_online"]: return False, "Soundbox still offline"
    if s["intent"] == "kyc_bank" and not snap["bank_verified"]: return False, "bank not verified"
    return True, ""

def llm_agent(s, text, proactive=False):
    m = MERCHANTS[s["merchant"]]; s["status"] = "DIAGNOSING"; s["llm"] = True
    pre = next((k for k, p in INTENTS.items() if re.search(p, text, re.I)), None)
    hint = {"settlement": "BANK_CHANGE_HOLD" if check_settlement_status(s["merchant"])["bank_recently_changed"] else "OTHER",
            "soundbox": DEVICES[m["soundbox"]].get("fault") or "OK"}.get(pre, "")
    cached = CACHE.get(signature(pre, hint)); s["intent"] = pre or "other"
    if cached: s["cache_hit"] = True; cached["hits"] += 1
    s["sop"] = rag(text)
    audit(s, "UNDERSTAND", f"{'⚡ CACHE_HIT — ' if cached else ''}Live agent: {LLM_PROVIDER}/{LLM_MODEL}", {"query": text, "proactive": proactive})
    if s["sop"]: audit(s, "DIAGNOSE", f"SOP retrieved: {s['sop']['id']}", s["sop"])
    with LOCK:
        db.execute("INSERT INTO tix VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET intent=excluded.intent, cache=excluded.cache",
                   (s["id"], s["merchant"], s["intent"], "OPEN", 1, int(s["cache_hit"]), 0, 0, s["opened"], None)); db.commit()
    hist = [{"role": "user" if x["from"] == "merchant" else "assistant", "content": x["text"].replace("🎤 ", "")} for x in [y for y in MSGS if y.get("merchant") == s["merchant"]][-9:-1] if x.get("text")]
    ask = text
    owed = [(k, t) for k, t in m["txns"].items() if t["debited"] and t["status"] == "FAILED" and not t["refunded"]]
    if len(owed) > 1 and re.search(r"\b(saare|sare|sab|sabhi|dono|all|both)\b", text, re.I):  # merchant picked all of them
        ask += "\n[SYSTEM] The merchant means ALL of these failed, customer-debited payments. Call process_refund for each now: " + ", ".join(f"txn_id={k} amount={t['amount']}" for k, t in owed)
        audit(s, "DIAGNOSE", f"Merchant chose ALL {len(owed)} failed payments", [k[-4:] for k, _ in owed])
    s["llm_msgs"] = [{"role": "system", "content": llm_system(s, s["sop"], cached)}, *hist, {"role": "user", "content": ask}]
    s["ctx"] = lambda tool, a: f"{m['name']}: live agent requests {tool}({', '.join(f'{k}={v}' for k, v in a.items())}). Query: '{text}'"
    try: llm_loop(s)
    except Exception as e:
        audit(s, "HUMAN", f"⚠ LLM unavailable ({e}) → stage-safe fallback", str(e)); s["llm"] = False
        return run_agent(s, text, proactive, live=False)

def llm_resume(s, ap, res):
    s["llm_msgs"].append({"role": "user", "content": f"[SYSTEM] A human approved {ap['tool']}. Result: {json.dumps(res, ensure_ascii=False, default=str)}. Verify the new state with a read tool, then confirm to the merchant."})
    s["status"] = "EXECUTING"
    try: llm_loop(s, acted=True)
    except Exception as e: reply(s, f"{ap['tool']} ho gaya ✅"); audit(s, "HUMAN", f"⚠ LLM unavailable on resume ({e})", str(e)); resolve(s, "Approved")

# ---------------- ML-first router: model decides, code executes, LLM only speaks the user's language ----------------
def verify_utr(uid, txn_id):
    t = CUSTOMERS[uid]["txns"][txn_id]
    return {"txn": txn_id, "debited": t["debited"], "merchant_credited": t["credited"], "npci_status": "DEEMED" if BANK_STATUS != "UP" else "PENDING", "bank": BANK_STATUS}

def passbook_add(uid, kind, title, amount, ref=""):
    """Every money event becomes its own passbook line (CREDIT / DEBIT / INFO), like a real bank passbook."""
    CUSTOMERS[uid].setdefault("ledger", []).append({"kind": kind, "title": title, "amount": amount, "ref": ref, "time": datetime.now().strftime("Today %H:%M"), "ts": time.time()})

def instant_refund(uid, txn_id, amount):
    t = CUSTOMERS[uid]["txns"][txn_id]
    if t["refunded"]: return {"ok": False, "error": "IDEMPOTENT_BLOCK: already refunded"}
    t.update(refunded=True, status="REFUNDED", rrn="RF" + uuid.uuid4().hex[:8].upper())
    passbook_add(uid, "CREDIT", f"Refund from {t['to']}", amount, "RRN " + t["rrn"])
    return {"ok": True, "txn": txn_id, "amount": amount, "rrn": t["rrn"]}

def check_emi(uid): return dict(CUSTOMERS[uid]["emi"])

def waive_late_fee(uid):
    e = CUSTOMERS[uid]["emi"]
    if not e["waived"]: passbook_add(uid, "INFO", f"Late fee waived · {e['loan']}", e["late_fee"], "Saathi waiver")
    e["waived"] = True; return {"waived": e["late_fee"], "reason": "first delay, good history"}

def send_emi_payment_link(uid):
    return {"link": f"https://paytm.me/emi/{uuid.uuid4().hex[:6]}", "amount": CUSTOMERS[uid]["emi"]["emi"]}

def freeze_account(uid):
    CUSTOMERS[uid]["frozen"] = True; return {"frozen": True, "upi_blocked": True, "cards_blocked": True}

def fraud_case_handoff(uid, txn_id, amount):
    case = "FRD-" + uuid.uuid4().hex[:5].upper()
    passbook_add(uid, "CREDIT", "Provisional credit · fraud case", amount, case)
    return {"case": case, "owner": "risk-02 (Meera)", "provisional_credit": amount, "chargeback": "RAISED"}

def check_ledger_and_kyc(mid, settlement_id):
    st = MERCHANTS[mid]["settlements"][settlement_id]
    return {"ledger_match": True, "kyc": "GREEN" if MERCHANTS[mid]["bank"]["verified"] else "PENDING", "hold_reason": st.get("reason"), "bank": BANK_STATUS}

def release_instant_settlement(mid, settlement_id, amount, ml_risk="LOW"):
    st = MERCHANTS[mid]["settlements"][settlement_id]
    if st["status"] == "SETTLED": return {"ok": False, "error": "IDEMPOTENT_BLOCK: already settled"}
    st.update(status="SETTLED", utr="UTR" + uuid.uuid4().hex[:10].upper()); return {"ok": True, "utr": st["utr"], "amount": amount}

def segment_repeat_buyers(mid, days=90):
    seg = [b for b in buyers(mid) if b["visits_90d"] >= 3 and b["last_visit_days"] <= days]
    return {"count": len(seg), "by_lang": {l: sum(b["lang"] == l for b in seg) for l in BUYER_LANGS if any(b["lang"] == l for b in seg)},
            "avg_ticket": round(sum(b["avg_ticket"] for b in seg) / max(1, len(seg)))}

def send_whatsapp_campaign(mid, offer, count, avg_ticket):
    cid = "CMP-" + uuid.uuid4().hex[:4].upper()
    CAMPAIGNS[cid] = {"id": cid, "merchant": mid, "offer": offer, "sent": count, "avg_ticket": avg_ticket, "start": time.time()}
    return {"campaign": cid, "sent": count, "channel": "WhatsApp (opted-in only)", "personalised_langs": True}

SESSIONS_ML = {}
FRAUD_WORDS = r"fraud|stole|steal|chori|unauthori|without (my )?permission|didn.?t make|did not make|don.?t recogni|nahi kiya|maine nahi|hack|scam|bina bataye|nikaal|नहीं किया|चोरी|பண்ணல|చేయలేదు|করিনি|केले नाही"
ML_TOOLS = {"verify_utr", "instant_refund", "check_emi", "waive_late_fee", "send_emi_payment_link", "freeze_account", "fraud_case_handoff",
            "check_ledger_and_kyc", "release_instant_settlement", "segment_repeat_buyers", "send_whatsapp_campaign"}
TOOLS.update({n: globals()[n] for n in ML_TOOLS})
_tier_base = tier
def tier(tool, args, mid="M1001"):
    if tool == "instant_refund": return ("NEEDS_APPROVAL", f"Refund ≥ ₹{AUTO_LIMIT}") if args.get("amount", 0) >= AUTO_LIMIT else ("AUTO", "Small refund, bank degraded, no disputes")
    if tool == "release_instant_settlement": return ("AUTO", "KYC green + ML risk LOW") if args.get("ml_risk") == "LOW" else ("NEEDS_APPROVAL", "ML risk not LOW")
    if tool == "fraud_case_handoff": return "NEEDS_APPROVAL", "Fraud — human teammate must own the case"
    if tool in ("freeze_account", "waive_late_fee", "send_emi_payment_link", "send_whatsapp_campaign") or tool in ML_TOOLS: return "AUTO", "Protective / reversible / informational"
    return _tier_base(tool, args, mid)

def campaign_live(c):
    el = time.time() - c["start"]; sent = c["sent"]
    paid = min(int(sent * 0.22), int(el / 4)); clicked = min(int(sent * 0.55), int(el / 1.5) + paid)
    return {**c, "delivered": sent, "clicked": clicked, "paid": paid, "gmv": paid * c["avg_ticket"], "conv_pct": round(100 * paid / max(1, sent), 1)}

LANG_HINT = {"Tamil": "Tamil script", "Telugu": "Telugu script", "Bengali": "Bengali script", "Marathi": "Marathi in Devanagari", "Hindi": "Hindi in Devanagari",
             "Hinglish": "Hinglish in Roman script", "English": "simple English"}

def speak(s, draft, d, **kw):
    """LLM only localises the already-decided action; falls back to the draft if no LLM."""
    text = draft
    if LLM_KEY:
        try:
            msg, tk, _ = llm_chat([{"role": "system", "content": f"You are Paytm's support teammate. Rewrite the message for a Paytm {d['persona']} in {LANG_HINT.get(d['lang'], 'Hinglish')}. "
                                   f"{'First sentence must calm their fear. ' if d['sentiment'] >= 0.5 else ''}Max 45 words. Keep every number, ID and link exact. Output only the message."},
                                  {"role": "user", "content": draft}], tools=False)
            text = (msg.get("content") or "").strip() or draft; s["tokens"] += tk
        except Exception as e: audit(s, "HUMAN", f"⚠ LLM phrasing unavailable ({str(e)[:60]}) — sent draft", "")
    return reply(s, text, **kw)

def open_ticket(s, intent):
    s["intent"] = intent
    with LOCK:
        db.execute("INSERT INTO tix VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET intent=excluded.intent", (s["id"], s["merchant"], intent, "OPEN", 1, 0, 0, 0, s["opened"], None)); db.commit()

def handoff(s, why, text):
    s["status"] = "WAITING_APPROVAL"; s["auto"] = False
    audit(s, "HUMAN", f"🙋 Handoff: {why}", text); staff_notify(f"Handoff {s['id']}: {why} — “{text[:80]}”")
    return reply(s, "Main aapka case abhi ek senior teammate ko de raha hoon, poori jaankari ke saath — aapko dobara kuch batana nahi padega 🙏")

def ml_route(s, text, proactive=False):
    uid = s["merchant"]; cust = uid in CUSTOMERS; u = user(uid)
    d = ml_engine.decide(text, "Customer" if cust else "Merchant", disputes=u.get("disputes", 0), bank=BANK_STATUS)
    if s.get("forced_issue"):
        f = s["forced_issue"]; d.update(issue=f, issue_conf=1.0, persona_expected=ml_engine.PERSONA[f], forced_by=APPROVER,
                                         action=ml_engine.policy(f, d["amount"] or 0, d["sentiment"], u.get("disputes", 0), BANK_STATUS))
        if d["action"] == "FREEZE_AND_HUMAN_HANDOFF": d["risk"] = "HIGH"
        text = text + " (agent: confirmed " + f + ")" if f == "Fraud_Unauthorized_Txn" else text
    s["ml"] = d; SESSIONS_ML[uid] = d; ML_LOG.append({**d, "ticket": s["id"], "user": uid, "ts": time.time()})
    audit(s, "UNDERSTAND", f"🤖 ML {d['latency_ms']}ms: {d['issue']} → {d['action']} · risk {d['risk']} ({d['risk_score']}) · {d['lang']} · sentiment {d['sentiment']}", d)
    if cust and not d["amount"] and u.get("txns"): d["amount"] = list(u["txns"].values())[-1]["amount"]  # amount from account, never ₹0
    fraud_said = bool(re.search(FRAUD_WORDS, text, re.I)) or s.get("forced_issue") == "Fraud_Unauthorized_Txn"
    if cust and (d["issue_conf"] < 0.6 or (d["action"] == "FREEZE_AND_HUMAN_HANDOFF" and d["issue"] == "Fraud_Unauthorized_Txn" and not fraud_said)):
        open_ticket(s, "clarify"); s["status"] = "NEEDS_INFO"
        audit(s, "DIAGNOSE", f"🛑 Guardrail: not acting on a guess (ML {d['issue']} {d['issue_conf']}, fraud words={fraud_said}) — asking user", d)
        return speak(s, "Main sahi madad karna chahta hoon. Kya hua hai?", d, options=["Payment fail hua, paise kat gaye — refund chahiye", "Ye payment maine nahi kiya (fraud)", "EMI ke baare mein"]) or True
    if d["issue_conf"] < 0.6: return False  # not sure -> let the conversational agent handle it, never act on a guess
    if (d["persona_expected"] == "Customer") != cust: return False  # e.g. merchant says "customer ka paisa kata" -> merchant flows
    act = d["action"]; s["ctx"] = lambda tool, a: f"{u['name']} ({'customer' if cust else 'merchant'}, {d['lang']}, sentiment {d['sentiment']}): {d['issue']} ₹{d['amount'] or a.get('amount', 0):,}; ML risk {d['risk']} {d['risk_score']}. Asks: {tool}."
    if act == "FREEZE_AND_HUMAN_HANDOFF":
        open_ticket(s, "fraud" if cust else d["issue"])
        if cust: call(s, "freeze_account")
        txn = next(iter(u.get("txns", {})), "N/A"); amt = d["amount"] or (u["txns"][txn]["amount"] if txn in u.get("txns", {}) else 0)
        call(s, "fraud_case_handoff", txn_id=txn, amount=amt)
        HANDOFFS.append({"ticket": s["id"], "risk": d["risk"], "latency_ms": int((time.time() - s["opened"]) * 1000)})
        return speak(s, f"Aapka paisa surakshit hai. Maine aapka account turant freeze kar diya hai taaki aur paisa na kate. ₹{amt:,} ka case ek senior fraud expert ko de diya hai, woh 10 minute mein call karenge." if cust
                     else "Is case ko main senior risk teammate ko de raha hoon, poori summary ke saath.", d) or True
    if cust and act == "AUTO_REFUND_INSTANT":
        open_ticket(s, "upi_pending"); c = CUSTOMERS[uid]
        txn = next((k for k, t in c["txns"].items() if t["status"] == "PENDING" and (not d["amount"] or t["amount"] == d["amount"])), None)
        if not txn:
            last = next(reversed(c["txns"].items()), None); s["status"] = "NEEDS_INFO"
            if not last: return speak(s, "Aapke account mein koi atka hua payment nahi mila. UPI ref number bhej dijiye, main turant check karta hoon.", d) or True
            k, t = last; audit(s, "DIAGNOSE", f"No failed/pending txn; latest {k} is {t['status']} → confirming with user", t)
            return speak(s, f"Maine check kiya — aapka last payment ₹{t['amount']:,} {t['to']} ko ({t['time']}) {t['status']} hua hai, fail nahi. Kya ye payment aapne kiya tha?", d,
                         options=["Haan, maine kiya tha", "Nahi, ye payment maine nahi kiya (fraud)"]) or True
        t = c["txns"][txn]; v = call(s, "verify_utr", txn_id=txn)
        audit(s, "DIAGNOSE", f"Debited={v['debited']} · merchant credited={v['merchant_credited']} · bank {BANK_STATUS} → refund path", v)
        r = call(s, "instant_refund", txn_id=txn, amount=t["amount"])
        if r is None: return speak(s, f"Ghabraiye mat, aapka ₹{t['amount']:,} surakshit hai. Refund senior ki approval ke baad turant ho jayega.", d) or True
        chk = CUSTOMERS[uid]["txns"][txn]; s["diff"] = {"before": {"status": "PENDING"}, "after": {"status": chk["status"], "rrn": chk.get("rrn")}}
        audit(s, "VERIFY", f"{txn}: PENDING → {chk['status']} (RRN {chk.get('rrn')})", s["diff"])
        speak(s, f"Ghabraiye mat, aapka paisa surakshit hai. Bank server slow tha, isliye ₹{t['amount']:,} {t['to']} tak nahi pahuncha. Maine abhi refund kar diya, RRN {chk['rrn']}. 2 ghante mein account mein dikhega.", d, voice=True)
        resolve(s, "Instant refund verified"); return True
    if cust and act == "SEND_EMI_LINK_WITH_REASSURANCE":
        open_ticket(s, "emi"); e = call(s, "check_emi")
        if e["paid"]:  # already paid: reassure, never ask for money twice
            audit(s, "VERIFY", "EMI already PAID for this cycle — no payment link sent", e)
            speak(s, f"Chinta mat kijiye — is mahine ki EMI {inr(e['emi'])} aap pehle hi bhar chuke hain ✅ Koi late fee nahi lagegi, aapka credit score safe hai. Agli EMI ka reminder main time pe bhej dunga.", d)
            resolve(s, "EMI already paid"); return True
        w = call(s, "waive_late_fee") if CUSTOMERS[uid]["disputes"] <= 1 else None
        link = call(s, "send_emi_payment_link")
        speak(s, f"Chinta mat kijiye, aapki EMI abhi bounce nahi hui hai. ₹{e['late_fee']} late fee maaf kar di hai. Neeche button se ₹{e['emi']:,} abhi bhar dijiye, bas ek click.", d,
              cta={"label": f"💳 Pay ₹{e['emi']:,} now", "product": "EMI", "amount": e["emi"]})
        s["status"] = "NEEDS_INFO"; return True
    if not cust and act == "TRIGGER_WHATSAPP_CAMPAIGN":
        open_ticket(s, "campaign"); seg = call(s, "segment_repeat_buyers", days=90)
        audit(s, "DIAGNOSE", f"Segment: {seg['count']} repeat buyers (≥3 visits, last 90d) across {len(seg['by_lang'])} languages — awaiting merchant OK", seg)
        langs = ", ".join(f"{l} {n}" for l, n in seg["by_lang"].items())
        speak(s, f"Aapke {seg['count']} regular customers hain (pichhle 90 din mein 3+ baar aaye). Unhe unki bhasha mein ({langs}) 'Diwali: 10% off + Paytm cashback' ka WhatsApp aur payment link bhej doon? Sirf opted-in customers ko jayega.", d,
              cta={"label": f"📣 Haan, {seg['count']} customers ko bhejo", "product": "CAMPAIGN", "amount": seg["count"]})
        s["campaign_seg"] = seg; s["status"] = "NEEDS_INFO"; return True
    if not cust and act == "RELEASE_INSTANT_SETTLEMENT":
        st = check_settlement_status(uid)
        if st["bank_recently_changed"]: s.pop("ml_handled", None); return False  # bank-change hold -> existing approval flow
        held = {k: v for k, v in MERCHANTS[uid]["settlements"].items() if v["status"] != "SETTLED"}
        if not held: return False
        open_ticket(s, "settlement")
        for sid_, v in held.items():
            k = call(s, "check_ledger_and_kyc", settlement_id=sid_)
            if k["kyc"] != "GREEN" or BANK_STATUS == "DOWN":
                speak(s, f"₹{v['amount']:,} ka settlement {k['hold_reason'] or 'review'} ki wajah se ruka hai. Bank server {BANK_STATUS} hai — aaj 4 PM ke batch mein pakka aa jayega, main khud confirm karunga.", d); s["status"] = "NEEDS_INFO"; return True
            before_status = v["status"]
            r = call(s, "release_instant_settlement", settlement_id=sid_, amount=v["amount"], ml_risk=d["risk"])
            if r is None: return speak(s, f"Ledger aur KYC theek hai. ₹{v['amount']:,} instant release ke liye senior approval le raha hoon.", d) or True
            after = MERCHANTS[uid]["settlements"][sid_]; s["diff"] = {"before": {"status": before_status}, "after": {"status": after["status"], "utr": after.get("utr")}}
            audit(s, "VERIFY", f"{sid_}: {before_status} → {after['status']} · UTR {after.get('utr')}", s["diff"])
            hello = f"Namaste {u['owner'].split()[0]} ji, aapke bolne se pehle hi bata doon — " if proactive else ""
            speak(s, f"{hello}Ledger match aur KYC green hai, isliye ₹{v['amount']:,} ka settlement abhi instant release kar diya ✅ UTR {after['utr']}. 30 min mein bank mein dikhega.", d, voice=True)
        resolve(s, "Instant settlement verified"); return True
    return False

def ml_resume(s, ap):
    tool, a = ap["tool"], ap["args"]
    res = TOOLS[tool](s["merchant"], **a); s["tools"] += 1; audit(s, "ACT", f"✔ {tool} (human-approved)", {"tool": tool, "args": a, "result": res})
    d = s.get("ml") or {"persona": "Customer", "lang": "Hinglish", "sentiment": 0}
    if tool == "fraud_case_handoff":
        audit(s, "VERIFY", f"Case {res['case']} owned by {res['owner']} · provisional credit ₹{res['provisional_credit']:,}", res)
        speak(s, f"{res['owner'].split('(')[-1].rstrip(')')} ji ne aapka case {res['case']} le liya hai. ₹{res['provisional_credit']:,} ka provisional credit 24 ghante mein milega. Account surakshit hai.", d, voice=True)
        s["status"] = "ESCALATED"; return
    if tool in ("instant_refund", "release_instant_settlement"):
        audit(s, "VERIFY", f"{tool} verified", res); speak(s, f"Approval mil gaya, ₹{a['amount']:,} process ho gaya ✅", d, voice=True); resolve(s, "Approved & verified")

def launch_campaign(s):
    seg = s.get("campaign_seg") or segment_repeat_buyers(s["merchant"])
    for x in MSGS:
        if x.get("cta") and x.get("merchant") == s["merchant"]: x["cta_done"] = True
    audit(s, "HUMAN", f"👍 Merchant approved broadcast to {seg['count']} customers", seg)
    cmp_ = call(s, "send_whatsapp_campaign", offer="Diwali: 10% off + Paytm cashback", count=seg["count"], avg_ticket=seg["avg_ticket"])
    reply(s, f"Bhej diya ✅ {seg['count']} customers ko unki bhasha mein offer gaya. Kitne ne khola aur kitne ne pay kiya, neeche live dikh raha hai 👇", kind="campaign")
    MSGS[-1]["campaign"] = cmp_["campaign"]; resolve(s, f"Campaign {cmp_['campaign']} live"); return view(s)

def pay_emi(s):
    c = CUSTOMERS[s["merchant"]]; e = c["emi"]
    if e["paid"]:  # idempotent: an old Pay button can never charge twice
        audit(s, "ACT", "🛑 Duplicate EMI payment blocked (already PAID)", e)
        reply(s, f"Ye EMI {inr(e['emi'])} pehle hi bhar di gayi hai ✅ Dobara koi paisa nahi kata."); return view(s)
    for x in MSGS:
        if x.get("cta") and x.get("merchant") == s["merchant"]: x["cta_done"] = True
    e["paid"] = True; passbook_add(s["merchant"], "DEBIT", f"EMI paid · {e['loan']}", e["emi"], "EMI-" + uuid.uuid4().hex[:6].upper())
    audit(s, "ACT", f"💳 EMI ₹{e['emi']:,} paid via link", e)
    audit(s, "VERIFY", f"Loan ledger: EMI {e['due']} → PAID · late fee waived={e['waived']}", e)
    speak(s, f"Payment mil gaya ✅ ₹{e['emi']:,} ki EMI bhar di gayi, koi late fee nahi lagegi. Aapka credit score safe hai.", s.get("ml") or {"persona": "Customer", "lang": c["lang"], "sentiment": 0})
    resolve(s, "EMI paid in same turn"); return view(s)

def exec_metrics(rows):
    res = [r for r in rows if r[0] in ("VERIFIED", "CLOSED")]
    high = [x for x in ML_LOG if x["risk"] == "HIGH"]
    camp = [campaign_live(c) for c in CAMPAIGNS.values()]
    queue = [{"id": a["id"], "ticket": a["ticket"], "tool": a["tool"], "waiting_s": int(time.time() - a["created"]),
              "sentiment": (a.get("ml") or {}).get("sentiment"), "ml_risk": (a.get("ml") or {}).get("risk"), "risk_score": (a.get("ml") or {}).get("risk_score"),
              "lang": (a.get("ml") or {}).get("lang"), "actions": a.get("actions", []), "summary": a["summary"]} for a in APPROVALS.values() if a["status"] == "PENDING"]
    return {"loop_closure_pct": round(100 * sum(r[1] for r in res) / len(rows)) if rows else 0, "tickets": len(rows), "closed_autonomously": sum(r[1] for r in res),
            "ml_decisions": len(ML_LOG), "ml_avg_ms": round(statistics.mean(x["latency_ms"] for x in ML_LOG), 1) if ML_LOG else None,
            "high_risk_caught": len(high), "handoffs": len(HANDOFFS), "handoff_ms": round(statistics.mean(h["latency_ms"] for h in HANDOFFS)) if HANDOFFS else None,
            "queue": queue, "campaigns": camp, "campaign_gmv": sum(c["gmv"] for c in camp), "campaign_paid": sum(c["paid"] for c in camp),
            "lang_mix": {l: sum(x["lang"] == l for x in ML_LOG) for l in {x["lang"] for x in ML_LOG}}, **business_metrics(camp)}

def business_metrics(camp):
    """Business view for the Paytm team: money flowing through merchants and what Saathi added."""
    open_t = {}
    for x in SESSIONS.values():
        if x["status"] not in ("VERIFIED", "CLOSED", "REASSIGNED", "ANSWERED"): open_t[x["merchant"]] = open_t.get(x["merchant"], 0) + 1
    rows = []
    for k, m in MERCHANTS.items():
        upi = sum(t["amount"] for t in m["txns"].values() if t["status"] == "SUCCESS"); cash = sum(c["amount"] for c in m.get("cash", []))
        rows.append({"id": k, "name": m["name"], "owner": m["owner"], "city": m.get("city", ""), "gmv_month": m["monthly_volume"], "upi_today": upi, "cash_today": cash,
                     "settled": sum(x["amount"] for x in m["settlements"].values() if x["status"] == "SETTLED"),
                     "held": sum(x["amount"] for x in m["settlements"].values() if x["status"] != "SETTLED"),
                     "soundbox_online": DEVICES[m["soundbox"]]["online"], "open": open_t.get(k, 0),
                     "products": [o["item"] for o in m.get("orders", [])]})
    orders = [o for m in MERCHANTS.values() for o in m.get("orders", [])]
    refunds = sum(t["amount"] for m in MERCHANTS.values() for t in m["txns"].values() if t["refunded"]) + sum(t["amount"] for c in CUSTOMERS.values() for t in c["txns"].values() if t["refunded"])
    return {"biz": {"gmv_month": sum(r["gmv_month"] for r in rows), "upi_today": sum(r["upi_today"] for r in rows), "cash_today": sum(r["cash_today"] for r in rows),
                    "settled": sum(r["settled"] for r in rows), "held": sum(r["held"] for r in rows), "refunds": refunds,
                    "loans": sum(o["amount"] for o in orders if o["item"].startswith("Business Loan")), "loan_n": sum(o["item"].startswith("Business Loan") for o in orders),
                    "soundbox_n": sum(o["item"].startswith("Soundbox") for o in orders), "campaign_gmv": sum(c["gmv"] for c in camp),
                    "soundbox_online": sum(r["soundbox_online"] for r in rows), "merchants": len(rows)},
            "merchant_rows": rows}

CANON = {"UPI_Pending": "payment fail hua, paise kat gaye, refund chahiye", "EMI_Bounce_Fear": "EMI bounce aur late fee ki chinta hai",
         "Fraud_Unauthorized_Txn": "ye payment maine nahi kiya, fraud hai", "Settlement_Hold": "settlement nahi aaya",
         "Soundbox_Sync_Issue": "soundbox bol nahi raha", "Festival_Promo_Broadcast": "regular customers ko Diwali offer bhej do"}

def ticket_row(x):
    d = x.get("ml") or {}; u = user(x["merchant"])
    return {"id": x["id"], "user": x["merchant"], "name": u.get("owner", u["name"]), "persona": "customer" if x["merchant"] in CUSTOMERS else "merchant",
            "query": x.get("query") or "(proactive / agent)", "issue": (d.get("issue") if d.get("acted", True) else None) or x.get("intent"), "action": d.get("action") if d.get("acted", True) else None, "risk": d.get("risk"),
            "forced": bool(d.get("forced_by")), "status": x["status"], "opened": x["opened"]}

@app.post("/api/tickets/{sid}/override")
def override(sid: str, issue: str):
    """Agent reclassifies a ticket; Saathi re-runs the loop for the corrected issue."""
    if sid not in SESSIONS or issue not in CANON: raise HTTPException(400, "unknown ticket or issue")
    old = SESSIONS[sid]
    if old["status"] not in ("VERIFIED", "CLOSED"): old["status"] = "REASSIGNED"
    s = session(None, old["merchant"]); s["forced_issue"] = issue; s["query"] = f"[agent] {issue} (was {sid})"
    audit(s, "HUMAN", f"👤 {APPROVER} reclassified {sid}: {(old.get('ml') or {}).get('issue') or old.get('intent')} → {issue}", {"from_ticket": sid, "issue": issue})
    run_agent(s, CANON[issue]); return view(s)

@app.post("/api/tickets/{sid}/close")
def close_manual(sid: str):
    s = SESSIONS.get(sid)
    if not s: raise HTTPException(404, "ticket")
    s["status"] = "CLOSED"; audit(s, "HUMAN", f"👤 {APPROVER} closed {sid} manually", {})
    s["resolved"] = s["resolved"] or time.time()
    with LOCK: db.execute("UPDATE tix SET status='CLOSED', auto=0, resolved=COALESCE(resolved, ?) WHERE id=?", (s["resolved"], sid)); db.commit()
    return view(s)

# ---------------- Soundbox app (merchants only): payment announcements + voice cash entry ----------------
HI_NUM = {"सौ": 100, "sau": 100, "हज़ार": 1000, "हजार": 1000, "hazaar": 1000, "hazar": 1000, "पचास": 50, "pachas": 50, "बीस": 20, "bees": 20, "दस": 10, "das": 10,
          "डेढ़ सौ": 150, "dedh sau": 150, "ढाई सौ": 250, "dhai sau": 250, "पांच सौ": 500, "पाँच सौ": 500, "paanch sau": 500, "do sau": 200, "दो सौ": 200, "teen sau": 300, "तीन सौ": 300}

def spoken_amount(t):
    n = ml_engine.extract_amount(t) or ml_engine.extract_amount(re.sub(r"[^\d]", " ", t))
    if n: return n
    for w, v in sorted(HI_NUM.items(), key=lambda x: -len(x[0])):
        if w in t.lower(): return v
    return None

def sb_say(m, text, kind):
    m.setdefault("sb_log", []).append({"text": text, "kind": kind, "ts": time.time()}); m["sb_log"] = m["sb_log"][-8:]
    return {"text": text, "voice": tts(text)}

def sb_audit(mid, event, detail):
    audit({"id": "SB-" + mid, "trace": [], "opened": time.time(), "tokens": 0, "phase": ""}, "ACT", event, detail)

@app.post("/soundbox/{mid}/cash")
async def soundbox_cash(mid: str, file: UploadFile | None = File(None), text: str = Form("")):
    """Merchant taps the Soundbox mic and says the cash amount; logged separately from Paytm money."""
    if mid not in MERCHANTS: raise HTTPException(404, "Soundbox is for merchants only")
    heard = text.strip() or (stt(await file.read(), file.filename) if file else "") or ""
    amt = spoken_amount(heard)
    if not amt: raise HTTPException(422, f"Amount samajh nahi aaya{' (heard: ' + heard + ')' if heard else ''} — boliye jaise '150 rupaye cash'")
    m = MERCHANTS[mid]; e = {"id": "CASH-" + uuid.uuid4().hex[:5].upper(), "amount": amt, "time": datetime.now().strftime("Today %H:%M"), "heard": heard}
    m.setdefault("cash", []).append(e); sb_audit(mid, f"💵 Cash entry ₹{amt:,} by voice (not Paytm money)", e)
    return {"entry": e, **sb_say(m, f"{amt} rupaye cash entry save ho gayi", "cash")}

@app.post("/soundbox/{mid}/simulate-payment")
def soundbox_payment(mid: str):
    if mid not in MERCHANTS: raise HTTPException(404, "merchant")
    m = MERCHANTS[mid]; d = DEVICES[m["soundbox"]]
    if not d["online"]: raise HTTPException(409, "Soundbox offline hai — payment aaya par announce nahi hua. Saathi se kahiye 'Soundbox bol nahi raha'.")
    import random as _r; amt = _r.choice([40, 85, 120, 250, 380, 640]); rrn = "4307" + str(_r.randint(10**7, 10**8 - 1))
    m["txns"][rrn] = {"amount": amt, "time": datetime.now().strftime("Today %H:%M"), "payer": _r.choice(["neha.v@okhdfc", "rahul.k@okaxis", "cust219@ybl"]), "status": "SUCCESS", "debited": True, "refunded": False}
    sb_audit(mid, f"🔊 Announced UPI ₹{amt} on {m['soundbox']}", {"rrn": rrn, "amount": amt})
    return sb_say(m, f"Paytm par {amt} rupaye prapt hue", "upi")

@app.post("/api/pin-check")
def pin_check(x_approver_pin: str = Header("")):
    if x_approver_pin != APPROVER_PIN: raise HTTPException(401, "bad approver PIN")
    return {"ok": True, "approver": APPROVER}

# ---------------- Staff WhatsApp: Saathi as a co-worker for Paytm employees ----------------
EMPLOYEES = {
    "9811000001": {"id": "E101", "name": "Priya Sharma", "role": "Support agent", "team": "Merchant Support", "can_approve": True},
    "9811000002": {"id": "E102", "name": "Arjun Mehta", "role": "Ops manager", "team": "Settlements Ops", "can_approve": True},
    "9811000003": {"id": "E103", "name": "Neha Kapoor", "role": "Risk analyst", "team": "Fraud & Risk", "can_approve": False},
}
STAFF = {}  # phone -> {"msgs": [...], "verified": bool, "subscribed": bool}

def staff_chat(phone):
    return STAFF.setdefault(phone, {"msgs": [], "verified": False, "subscribed": False})

def staff_say(phone, text, **kw):
    staff_chat(phone)["msgs"].append({"from": "saathi", "text": text, "ts": time.time(), **kw})

def staff_audit(emp, event, detail=""):
    audit({"id": "STAFF-" + (emp["id"] if emp else "UNKNOWN"), "trace": [], "opened": time.time(), "tokens": 0, "phase": ""}, "HUMAN", event, detail)

def staff_notify(text, aid=None):
    """Ping every verified, subscribed employee when a case needs a human."""
    for phone, ch in STAFF.items():
        if ch["verified"] and ch["subscribed"]:
            staff_say(phone, "🔔 " + text, alert=True, **({"approval": aid} if aid else {}))

def _open_tickets():
    return [x for x in SESSIONS.values() if x["status"] not in ("VERIFIED", "CLOSED", "REASSIGNED", "ANSWERED")]

def _who(uid):
    u = user(uid); return u.get("owner", u["name"])

def _find_user(text):
    t = text.lower()
    return next((k for k, v in {**MERCHANTS, **CUSTOMERS}.items()
                 if k.lower() in t or v["name"].lower().split()[0] in t or v.get("owner", v["name"]).lower().split()[0] in t), None)

STAFF_HELP = ("Main Saathi hoon, aapka teammate. Aap mujhse keh sakte hain:\n• *show open tickets* / *last 5 tickets*\n• *TK-XXXXXX* (ya list ke baad *3rd*) — ek ticket ka detail\n• *pending approvals*\n• *approve AP-XXXX <PIN>* / *reject AP-XXXX*\n"
              "• *status of Ramesh* (ya kisi bhi merchant/customer)\n• *today's summary*\n• *ping me* — jab bhi kisi case ko insaan chahiye, main WhatsApp karunga\n• *stop pings*")

def staff_reply(phone, text):
    ch = staff_chat(phone); emp = EMPLOYEES.get(phone); t = text.strip(); low = t.lower()
    ch["msgs"].append({"from": "staff", "text": t, "ts": time.time()})
    if not emp:
        staff_audit(None, f"⛔ Staff WhatsApp from unregistered number {mask(phone)}", t)
        return staff_say(phone, f"Ye number ({mask(phone)}) Paytm employee list mein nahi hai. Internal data share nahi kar sakta. Apne registered work number se message kijiye.")
    if not ch["verified"]:
        ch["verified"] = True; staff_audit(emp, f"✅ Staff verified by phone match: {emp['name']} ({emp['id']})", {"phone": mask(phone)})
        staff_say(phone, f"Verified ✅ {emp['name']} · {emp['role']}, {emp['team']} ({emp['id']}). Number employee list se match ho gaya.")
    m = re.search(r"\b(approve|reject|override)\b\s+(AP-[A-Z0-9]+)(?:\s+(\d{4}))?", t, re.I)
    if m:
        verb, aid, pin = m.group(1).lower(), m.group(2).upper(), m.group(3)
        ap = APPROVALS.get(aid)
        if not ap or ap["status"] != "PENDING": return staff_say(phone, f"{aid} pending nahi hai (shayad already handle ho gaya).")
        if not emp["can_approve"]:
            staff_audit(emp, f"⛔ {emp['name']} tried to {verb} {aid} without approval rights", ap["args"])
            return staff_say(phone, f"Aapke role ({emp['role']}) ko approve/reject ka access nahi hai. Main Priya ya Arjun ko ping kar deta hoon.")
        if verb == "approve" and pin != APPROVER_PIN:
            return staff_say(phone, f"Security ke liye PIN chahiye. Likhiye: *approve {aid} <PIN>*")
        staff_audit(emp, f"📱 {emp['name']} {verb}d {aid} via Staff WhatsApp", ap["args"])
        r = decide(aid, "approve" if verb == "approve" else "override", APPROVER_PIN)
        last = next((x["text"] for x in reversed(MSGS) if x.get("ticket") == ap["ticket"] and x["from"] == "saathi"), "")
        return staff_say(phone, f"{'✅ Approved' if verb == 'approve' else '🛑 Taken over'} {aid} ({ap['tool']}). Ticket {ap['ticket']} ab {r['status']}.\nMerchant ko gaya: “{last[:140]}”")
    if re.search(r"\b(stop|unsubscribe|mute)\b", low):
        ch["subscribed"] = False; staff_audit(emp, f"🔕 {emp['name']} stopped pings")
        return staff_say(phone, "Theek hai, pings band. Wapas chahiye toh *ping me* likhiye.")
    if re.search(r"\b(ping|notify|alert|batana|bata dena)\b", low):
        ch["subscribed"] = True; staff_audit(emp, f"🔔 {emp['name']} subscribed to human-needed pings")
        return staff_say(phone, "Done 🔔 Jab bhi koi case insaan ki zaroorat wala aayega (approval, fraud, escalation), main turant yahin message karunga.")
    if re.search(r"approval|pending|queue|waiting", low):
        aps = [a for a in APPROVALS.values() if a["status"] == "PENDING"]
        if not aps: return staff_say(phone, "Abhi koi approval pending nahi hai ✅")
        return staff_say(phone, f"{len(aps)} approval pending:\n" + "\n".join(
            f"• *{a['id']}* · {a['tool']} {('₹' + format(a['args']['amount'], ',')) if a['args'].get('amount') else ''} · {_who(SESSIONS[a['ticket']]['merchant'])}\n  {a['summary'][:110]}" for a in aps)
            + ("\nReply *approve <ID> <PIN>* ya *reject <ID>*" if emp["can_approve"] else ""))
    tid = re.search(r"TK-[A-Z0-9]{6}", t, re.I)
    nth = re.search(r"\b(\d+)(?:st|nd|rd|th)\b|#(\d+)|\b(first|second|third|fourth|fifth|pehla|pehle|doosra|dusra|teesra|tisra|chautha)\b", low)
    if not tid and nth and ch.get("last_list"):
        words = {"first": 1, "pehla": 1, "pehle": 1, "second": 2, "doosra": 2, "dusra": 2, "third": 3, "teesra": 3, "tisra": 3, "fourth": 4, "chautha": 4, "fifth": 5}
        i = int(nth.group(1) or nth.group(2) or words[nth.group(3)])
        if not 1 <= i <= len(ch["last_list"]): return staff_say(phone, f"Pichhli list mein sirf {len(ch['last_list'])} ticket the. *recent tickets* likhiye poori list ke liye.")
        tid = re.search(r"TK-[A-Z0-9]{6}", ch["last_list"][i - 1])
    if tid:
        x = SESSIONS.get(tid.group(0).upper())
        if not x: return staff_say(phone, f"{tid.group(0).upper()} naam ka koi ticket nahi mila. *recent tickets* likhiye list dekhne ke liye.")
        row_ = ticket_row(x); ap = next((a for a in APPROVALS.values() if a["ticket"] == x["id"] and a["status"] == "PENDING"), None)
        last = next((m_["text"] for m_ in reversed(MSGS) if m_.get("ticket") == x["id"] and m_["from"] == "saathi"), "—")
        return staff_say(phone, f"🎫 *{x['id']}* · {row_['name']} ({x['merchant']})\nIssue: {row_['issue'] or 'query'} · Status: *{x['status']}*\nOpened {datetime.fromtimestamp(x['opened']).strftime('%H:%M')} · said: “{(x.get('query') or '')[:90]}”\n"
                                   f"Saathi's last reply: “{last[:140]}”" + (f"\n⏳ Waiting on *{ap['id']}* ({ap['tool']}) — reply *approve {ap['id']} <PIN>*" if ap else ""))
    reasoning = LLM_KEY and re.search(r"urgent|priority|kyun|why|which|kaunsa|sabse|should i|kya karu", low)
    recent = re.search(r"\b(last|recent|latest|pichhle|saare|all)\b", low)
    if re.search(r"ticket|case|issue|complaint", low) and recent and not reasoning:
        n = int((re.search(r"\b(\d{1,2})\b", low) or [None, 10])[1]); lst = sorted(SESSIONS.values(), key=lambda x: -x["opened"])[:n]
        ch["last_list"] = [x["id"] for x in lst]
        return staff_say(phone, (f"Last {len(lst)} ticket:\n" + "\n".join(f"{i}. *{x['id']}* · {_who(x['merchant'])} · {ticket_row(x)['issue'] or 'query'} · {x['status']}" for i, x in enumerate(lst, 1))
                                 + "\nKisi ka detail: ticket ID ya *3rd* likhiye") if lst else "Abhi tak koi ticket nahi aaya.")
    if re.search(r"ticket|case|issue|complaint", low) and not reasoning:
        op = _open_tickets(); ch["last_list"] = [x["id"] for x in op]
        if not op: return staff_say(phone, f"Koi open ticket nahi ✅ Aaj total {len(SESSIONS)} tickets aaye, sab handle ho gaye.")
        return staff_say(phone, f"{len(op)} open ticket:\n" + "\n".join(
            f"• *{x['id']}* · {_who(x['merchant'])} · {(x.get('ml') or {}).get('issue') or x.get('intent') or 'query'} · {x['status']}\n  “{(x.get('query') or '')[:70]}”" for x in op))
    uid = _find_user(t)
    if uid or re.search(r"status|360|kaisa|details", low):
        if not uid: return staff_say(phone, "Kis merchant/customer ka? Naam ya ID likhiye, jaise *status of Ramesh* ya *C2003*.")
        p = profile(uid); mine = [x for x in SESSIONS.values() if x["merchant"] == uid]
        if p.get("persona") == "customer":
            info = f"👤 {p['name']} ({uid}, {p['city']}) · {'🔒 FROZEN' if p['frozen'] else 'active'} · disputes {p['disputes']}" + (f" · EMI ₹{p['emi']['emi']:,} {'paid' if p['emi']['paid'] else 'unpaid'}" if p.get("emi") else "")
        else:
            held = sum(x["amount"] for x in p["settlements"] if x["status"] != "SETTLED")
            info = f"🏪 {p['name']} · {p['owner']} ({uid}, {p['city']}) · GMV ₹{p['gmv']:,}/mo · Soundbox {'online' if p['soundbox_online'] else 'OFFLINE'} · " + (f"₹{held:,} on hold" if held else "settlements clear")
        return staff_say(phone, info + f"\nTickets: {len(mine)} (" + ", ".join(f"{x['id']} {x['status']}" for x in mine[-3:]) + ")" if mine else info + "\nKoi ticket nahi.")
    if re.search(r"summary|report|dashboard|metrics|aaj|today|kitna", low):
        rows = db.execute("SELECT status, auto FROM tix").fetchall(); b = business_metrics([campaign_live(x) for x in CAMPAIGNS.values()])["biz"]
        done = [r for r in rows if r[0] in ("VERIFIED", "CLOSED")]
        return staff_say(phone, f"📊 Aaj ka summary\n• Tickets {len(rows)} · resolved {len(done)} · bina insaan ke {sum(r[1] for r in done)}\n• Open {len(_open_tickets())} · approvals pending {sum(a['status'] == 'PENDING' for a in APPROVALS.values())}\n"
                                   f"• UPI today {inr(b['upi_today'])} · settled {inr(b['settled'])} · on hold {inr(b['held'])}\n• Loans {inr(b['loans'])} · refunds {inr(b['refunds'])} · cash logged {inr(b['cash_today'])}")
    if re.search(r"^(hi|hello|hey|help|namaste|menu)\b", low): return staff_say(phone, f"Namaste {emp['name'].split()[0]} 👋\n" + STAFF_HELP)
    if LLM_KEY:  # free-form question: answer from live ops context only
        try:
            ctx = {"recent_tickets": [ticket_row(x) for x in sorted(SESSIONS.values(), key=lambda x: -x["opened"])][:15], "pending_approvals": [{k: a[k] for k in ("id", "tool", "summary")} for a in APPROVALS.values() if a["status"] == "PENDING"]}
            msg, _, _ = llm_chat([{"role": "system", "content": f"You are Saathi, an internal teammate for Paytm employee {emp['name']} ({emp['role']}). Answer briefly (max 60 words, Hinglish ok) using ONLY this live ops data; if it is not there, say so. Never invent commands — the only commands are: show open tickets, recent tickets, <ticket ID>, pending approvals, approve/reject AP-ID, status of <name>, today's summary, ping me. Data: {json.dumps(ctx, ensure_ascii=False, default=str)}"},
                                  {"role": "user", "content": t}], tools=False)
            if msg.get("content"): return staff_say(phone, msg["content"].strip())
        except Exception: pass
    return staff_say(phone, "Ye main abhi nahi kar sakta.\n" + STAFF_HELP)

class StaffMsg(BaseModel):
    phone: str; text: str

@app.post("/staff/chat")
def staff_chat_api(m: StaffMsg):
    staff_reply(re.sub(r"\D", "", m.phone)[-10:], m.text); return staff_state()

@app.get("/staff/state")
def staff_state():
    return {"employees": [{"phone": p, "masked": mask(p), **e, "verified": STAFF.get(p, {}).get("verified", False), "subscribed": STAFF.get(p, {}).get("subscribed", False)} for p, e in EMPLOYEES.items()],
            "chats": STAFF}

# ---------------- Live payment: customer pays a merchant, Paytm settles to the merchant's bank ----------------
# Illustrative merchant discount rates (MDR): UPI P2M is zero-MDR; wallet/card rates are demo values, not Paytm's tariff.
MDR = {"UPI": 0.0, "WALLET": 0.0199, "CARD": 0.02}
GST_ON_FEE = 0.18

class PayReq(BaseModel):
    customer_id: str = "C2001"; merchant_id: str = "M1003"; amount: int = 1100; method: str = "UPI"; outcome: str = "success"; note: str = ""; coupon: str = ""

@app.post("/pay")
def live_pay(r: PayReq):
    if r.customer_id not in CUSTOMERS or r.merchant_id not in MERCHANTS: raise HTTPException(404, "customer or merchant")
    if r.amount <= 0 or r.amount > 200000: raise HTTPException(422, "amount must be between ₹1 and ₹2,00,000")
    method = r.method.upper() if r.method.upper() in MDR else "UPI"
    cp, disc = apply_coupon(r.merchant_id, r.coupon, r.amount) if r.coupon.strip() else (None, 0)
    amt = round(r.amount - disc, 2); amt = int(amt) if float(amt).is_integer() else amt  # what the customer actually pays
    c, m = CUSTOMERS[r.customer_id], MERCHANTS[r.merchant_id]; now = datetime.now().strftime("Today %H:%M")
    rrn = "4307" + uuid.uuid4().hex[:8].upper(); ok = r.outcome != "stuck"
    c["txns"]["UPI-" + rrn[-5:]] = {"amount": amt, "to": m["name"], "time": now, "status": "SUCCESS" if ok else "PENDING", "debited": True, "credited": ok, "refunded": False, "rrn_ref": rrn}
    fee = round(amt * MDR[method], 2); gst = round(fee * GST_ON_FEE, 2); net = round(amt - fee - gst, 2)
    if cp and ok: cp["redemptions"].append({"customer": c["name"], "bill": r.amount, "discount": disc, "paid": amt, "time": now})
    steps = ([{"key": "coupon", "label": f"Coupon {cp['code']} applied: bill ₹{r.amount:,} − ₹{disc:,g} = ₹{amt:,g}", "ok": True}] if cp else []) + [{"key": "debited", "label": f"₹{amt:,} debited from {c['name']}", "ok": True}]
    sb = None
    if ok:
        m["txns"][rrn] = {"amount": amt, "time": now, "payer": c["name"].split()[0].lower() + "@paytm", "status": "SUCCESS", "debited": True, "refunded": False, "method": method}
        m.setdefault("unsettled", []).append({"rrn": rrn, "gross": amt, "fee": fee, "gst": gst, "net": net, "method": method, "time": now})
        steps.append({"key": "paytm", "label": f"Paytm received ₹{amt:,} ({method}, ref ...{rrn[-4:]})", "ok": True})
        if DEVICES[m["soundbox"]]["online"]:
            sb = sb_say(m, f"Paytm par {amt} rupaye prapt hue", "upi"); steps.append({"key": "soundbox", "label": f"Soundbox {m['soundbox']}: “{sb['text']}”", "ok": True})
        else:
            steps.append({"key": "soundbox", "label": f"Soundbox {m['soundbox']} is offline — payment received but not announced", "ok": False})
        steps.append({"key": "ledger", "label": f"{m['name']} ledger +₹{amt:,}", "ok": True})
        steps.append({"key": "settle", "label": f"Next settlement (T+1): gross ₹{amt:,} − fee ₹{fee:,} − GST ₹{gst:,} = ₹{net:,}", "ok": None})
    else:
        steps.append({"key": "paytm", "label": "Bank didn't confirm — money debited but not credited to the merchant", "ok": False})
    audit({"id": "PAY-" + rrn[-6:], "trace": [], "opened": time.time(), "tokens": 0, "phase": ""}, "ACT",
          f"💸 Live payment {c['name']} → {m['name']} ₹{amt:,} via {method}: {'SUCCESS' if ok else 'STUCK (debited, not credited)'}", {"rrn": rrn, "fee": fee, "gst": gst, "net": net})
    return {"bill": r.amount, "discount": disc, "coupon": cp["code"] if cp else None, "ok": ok, "rrn": rrn, "method": method, "amount": amt, "fee": fee, "gst": gst, "net": net, "steps": steps, "soundbox": sb,
            "customer": c["name"], "merchant": m["name"], "complaint": None if ok else f"Maine {m['name']} ko ₹{amt:,} bheje, paisa kat gaya par unhe mila nahi"}

@app.post("/settle/{mid}")
def run_settlement(mid: str):
    """Settlement batch: Paytm pays the merchant's bank the day's collections minus MDR and GST on it."""
    if mid not in MERCHANTS: raise HTTPException(404, "merchant")
    m = MERCHANTS[mid]; batch = m.get("unsettled", [])
    if not batch: raise HTTPException(409, "Nothing to settle — no new payments since the last batch")
    g, f, t, n = (round(sum(x[k] for x in batch), 2) for k in ("gross", "fee", "gst", "net"))
    sid = "ST-" + uuid.uuid4().hex[:4].upper(); utr = "UTR" + uuid.uuid4().hex[:10].upper()
    m["settlements"][sid] = {"amount": n, "date": "today", "status": "SETTLED", "reason": "", "utr": utr, "gross": g, "fee": f, "gst": t, "count": len(batch)}
    m["unsettled"] = []
    audit({"id": sid, "trace": [], "opened": time.time(), "tokens": 0, "phase": ""}, "ACT", f"🏦 Settlement {sid} to {mask(m['bank']['acc'])}: ₹{g:,} − fee ₹{f:,} − GST ₹{t:,} = ₹{n:,}", {"utr": utr, "payments": len(batch)})
    return {"settlement": sid, "utr": utr, "gross": g, "fee": f, "gst": t, "net": n, "count": len(batch), "bank": mask(m["bank"]["acc"]), "merchant": m["name"]}

# ---------------- Merchant coupons: created by the merchant (or by Saathi on request), redeemed in payments ----------------
def _coupon_state(cp):
    if cp["status"] == "PAUSED": return "PAUSED"
    return "EXPIRED" if datetime.now().date().isoformat() > cp["valid_to"] else "ACTIVE"

def coupon_view(cp, detail=False):
    """Merchants see the count; Paytm staff (detail=True) also see who redeemed and the discount given."""
    v = {"code": cp["code"], "percent": cp["percent"], "max_discount": cp["max_discount"], "valid_from": cp["valid_from"], "valid_to": cp["valid_to"],
         "status": _coupon_state(cp), "used_by": len({r["customer"] for r in cp["redemptions"]}), "uses": len(cp["redemptions"]), "created_by": cp.get("created_by", "merchant")}
    if detail: v |= {"discount_given": round(sum(r["discount"] for r in cp["redemptions"]), 2), "redemptions": cp["redemptions"][-10:][::-1]}
    return v

def create_coupon(merchant_id, code, percent, valid_to, max_discount=0, created_by="Saathi"):
    code = re.sub(r"[^A-Z0-9]", "", str(code).upper())[:15]
    if not code: return {"ok": False, "error": "coupon code needed, e.g. DIWALI10"}
    if not 1 <= float(percent) <= 50: return {"ok": False, "error": "discount must be 1–50%"}
    try: to = datetime.fromisoformat(str(valid_to)[:10]).date().isoformat()
    except ValueError: return {"ok": False, "error": "valid_to must be a date like 2026-11-01"}
    cps = MERCHANTS[merchant_id].setdefault("coupons", {})
    if code in cps and _coupon_state(cps[code]) == "ACTIVE": return {"ok": False, "error": f"{code} already exists and is active"}
    cps[code] = {"code": code, "percent": float(percent), "max_discount": float(max_discount), "valid_from": datetime.now().date().isoformat(), "valid_to": to,
                 "status": "ACTIVE", "created_by": created_by, "redemptions": []}
    return {"ok": True, **coupon_view(cps[code])}

def list_coupons(merchant_id):
    return {"coupons": [coupon_view(c) for c in MERCHANTS[merchant_id].get("coupons", {}).values()]}

def set_coupon_status(merchant_id, code, status):
    cp = MERCHANTS[merchant_id].get("coupons", {}).get(str(code).upper())
    if not cp: return {"ok": False, "error": "no such coupon"}
    cp["status"] = "PAUSED" if str(status).upper().startswith("PAUS") else "ACTIVE"; return {"ok": True, **coupon_view(cp)}

def apply_coupon(mid, code, amount):
    cp = MERCHANTS[mid].get("coupons", {}).get(str(code).upper())
    if not cp: raise HTTPException(422, f"Coupon {code} doesn't exist for this merchant")
    if _coupon_state(cp) != "ACTIVE": raise HTTPException(422, f"Coupon {cp['code']} is {_coupon_state(cp).lower()}")
    off = amount * cp["percent"] / 100
    return cp, round(min(off, cp["max_discount"]) if cp["max_discount"] > 0 else off, 2)

TOOLS.update({f.__name__: f for f in (create_coupon, list_coupons, set_coupon_status)})
TOOL_SPECS.update({
    "create_coupon": ("Create a discount coupon customers can use when paying this merchant. Ask only for what is missing.", {"code": "string", "percent": "number", "valid_to": "string", "max_discount": "number"}, ["code", "percent", "valid_to"]),  # max_discount omitted = no cap
    "list_coupons": ("List the merchant's coupons with status and how many customers used each.", {}, []),
    "set_coupon_status": ("Pause or re-activate a coupon.", {"code": "string", "status": "string"}, ["code", "status"]),
})
LLM_TOOLS[:] = [{"type": "function", "function": {"name": n, "description": d, "parameters": {"type": "object", "properties": {k: {"type": t} for k, t in ps.items()}, "required": req}}}
                for n, (d, ps, req) in TOOL_SPECS.items()]
READ_ONLY.add("list_coupons")

class CouponReq(BaseModel):
    code: str; percent: float = 10; valid_to: str = ""; max_discount: float = 0  # 0 = no cap

@app.post("/coupons/{mid}")
def coupon_create_api(mid: str, r: CouponReq):
    if mid not in MERCHANTS: raise HTTPException(404, "merchant")
    res = create_coupon(mid, r.code, r.percent, r.valid_to or datetime.now().date().isoformat(), r.max_discount, created_by="merchant (app)")
    if not res["ok"]: raise HTTPException(422, res["error"])
    sb_audit(mid, f"🏷 Coupon {res['code']} created in app: {res['percent']:g}% off till {res['valid_to']}", res); return res

@app.post("/coupons/{mid}/{code}/{status}")
def coupon_status_api(mid: str, code: str, status: str):
    if mid not in MERCHANTS: raise HTTPException(404, "merchant")
    res = set_coupon_status(mid, code, status)
    if not res["ok"]: raise HTTPException(404, res["error"])
    sb_audit(mid, f"🏷 Coupon {res['code']} → {res['status']}", res); return res

class MLReq(BaseModel):
    text: str; persona: str = "Customer"; amount: int | None = None; disputes: int = 0; bank: str | None = None

@app.post("/api/ml-decide")
def ml_decide(r: MLReq):
    return ml_engine.decide(r.text, r.persona, r.amount, r.disputes, r.bank or BANK_STATUS)

@app.post("/api/bank-status/{st}")
def bank_status(st: str):
    global BANK_STATUS
    if st not in ("UP", "DEGRADED", "DOWN"): raise HTTPException(400, "UP|DEGRADED|DOWN")
    BANK_STATUS = st; return {"bank_status": st}

@app.post("/api/retrain")
def retrain(rows: int = 1500):
    ml_engine._M.clear(); rep_ = ml_engine.train(rows); ml_engine.load(); return rep_

@app.get("/api/audit-report")
def audit_report():
    f = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "EFFECTIVENESS_AUDIT.md")
    return {"markdown": open(f, encoding="utf-8").read() if os.path.exists(f) else "# Audit report missing"}

# ---------------- API ----------------
class Msg(BaseModel):
    text: str; merchant_id: str = "M1001"; session_id: str | None = None

def session(sid, merchant):
    if sid in SESSIONS and SESSIONS[sid]["status"] in ("OPEN", "DIAGNOSING"): return SESSIONS[sid]
    for x in SESSIONS.values():  # merchant replied -> earlier clarifying question is answered
        if x["merchant"] == merchant and x["status"] == "NEEDS_INFO": x["status"] = "ANSWERED"
    sid = "TK-" + uuid.uuid4().hex[:6].upper()
    SESSIONS[sid] = {"id": sid, "merchant": merchant, "status": "OPEN", "phase": "IDLE", "trace": [], "opened": time.time(), "resolved": None, "auto": True,
                     "cache_hit": False, "tokens": 0, "tools": 0, "sop": None, "diff": None, "ctx": lambda t, a: f"{t} {a}"}
    return SESSIONS[sid]

def handle(s, text, voice_in=False):
    s["query"] = text
    MSGS.append({"from": "merchant", "text": ("🎤 " if voice_in else "") + text, "ts": time.time(), "merchant": s["merchant"]}); run_agent(s, text); return view(s)

@app.post("/chat")
def chat(m: Msg):
    if m.merchant_id not in MERCHANTS and m.merchant_id not in CUSTOMERS: raise HTTPException(404, "user")
    return handle(session(m.session_id, m.merchant_id), m.text.replace("🎤", "").strip(), m.text.startswith("🎤"))

@app.post("/voice")
async def voice(file: UploadFile = File(...), merchant_id: str = Form("M1001"), hint: str = Form("")):
    if merchant_id not in MERCHANTS: raise HTTPException(404, "merchant")
    text = (stt(await file.read(), file.filename) or hint or "").strip()
    if not text: raise HTTPException(422, "Could not hear anything — check SARVAM_API_KEY or speak a bit louder")
    return handle(session(None, merchant_id), text, True)

@app.post("/webhook/{event}")
def webhook(event: str, merchant_id: str = "M1001"):
    """Proactive: a Paytm backend event arrives — Saathi messages the merchant FIRST."""
    m = MERCHANTS[merchant_id]
    if event == "settlement_failed":
        if all(v["status"] == "SETTLED" for v in m["settlements"].values()):  # don't stack a new hold on an existing one
            m["bank"].update(changed_days_ago=0, verified=False)
            m["settlements"]["ST-" + uuid.uuid4().hex[:4].upper()] = {"amount": 4200, "date": "today", "status": "FAILED", "reason": "Beneficiary account mismatch (bank changed)"}
        text = "EVENT settlement_failed on hold"
    elif event == "soundbox_offline":
        DEVICES[m["soundbox"]].update(online=False, fault="network", last_ping="9 min ago"); text = "EVENT soundbox offline"
    else: raise HTTPException(400, "event")
    s = session(None, merchant_id); audit(s, "UNDERSTAND", f"📡 Webhook {event} received — proactive outreach", {"event": event})
    run_agent(s, text, proactive=True); return view(s)

@app.post("/demo/soundbox_offline")
def sb_offline(merchant_id: str = "M1001"):
    DEVICES[MERCHANTS[merchant_id]["soundbox"]].update(online=False, fault="network", last_ping="14 min ago"); return {"ok": True}

@app.post("/approvals/{aid}/{decision}")
def decide(aid: str, decision: str, x_approver_pin: str = Header("")):
    ap = APPROVALS.get(aid)
    if not ap or ap["status"] != "PENDING": raise HTTPException(404, "no pending approval")
    s = SESSIONS[ap["ticket"]]
    if x_approver_pin != APPROVER_PIN:
        audit(s, "HUMAN", f"⛔ Approval attempt on {aid} rejected — bad approver PIN", {"aid": aid}); raise HTTPException(401, "bad approver PIN")
    if decision == "approve": ap["status"] = "APPROVED"; s["status"] = "EXECUTING"; resume(ap)
    else:
        ap["status"] = "OVERRIDDEN"; audit(s, "HUMAN", f"👤 Human override {aid} — agent takes over manually", ap["args"])
        reply(s, "Is case ko hamare senior agent ne apne haath mein liya hai, woh aapko call karenge 🙏"); s["status"] = "ESCALATED"
    return view(s)

def followup_one(s):
    snap = snapshot(s["merchant"]); ok = {"settlement": all(v == "SETTLED" for v in snap["settlements"].values()), "kyc_bank": snap["bank_verified"],
                                         "soundbox": snap["soundbox_online"]}.get(s["intent"], True)
    audit(s, "FOLLOWUP", f"⏩ +24h check-in: {'healthy' if ok else 'REGRESSED'}", snap)
    if ok:
        reply(s, {"settlement": "24 ghante baad check kiya — aaj ke settlements bhi time pe aa rahe hain ✅ Ticket band kar raha hoon.",
                  "soundbox": "24 ghante baad check kiya — Soundbox poore din online raha ✅ Ticket band kar raha hoon."}.get(s["intent"], "24 ghante baad check kiya — sab theek hai ✅ Ticket band."))
        MSGS[-1]["csat"] = s["id"]; s["status"] = "CLOSED"
        with LOCK: db.execute("UPDATE tix SET status='CLOSED' WHERE id=?", (s["id"],)); db.commit()
    else:
        s["status"] = "OPEN"; reply(s, "Follow-up mein issue wapas dikha, main dobara theek kar raha hoon.")

@app.post("/followup")
def followup():
    """Fast-forward: run Step 06 now (the background scheduler does it for real after FOLLOWUP_AFTER_S)."""
    due = [s for s in SESSIONS.values() if s["status"] == "VERIFIED"]
    if not due: raise HTTPException(404, "no verified tickets awaiting follow-up")
    for s in due: followup_one(s)
    return view(due[-1])

def scheduler():
    while True:
        time.sleep(5)
        try:
            for s in list(SESSIONS.values()):
                if s["status"] == "VERIFIED" and s["resolved"] and time.time() - s["resolved"] >= FOLLOWUP_AFTER_S: followup_one(s); save_world()
        except Exception as e: print("scheduler:", e)

@app.on_event("startup")
def _start():
    load_world(); ml_engine.decide("warm up", "Customer"); threading.Thread(target=scheduler, daemon=True).start()

@app.middleware("http")
async def persist(request: Request, call_next):
    r = await call_next(request)
    if request.method == "POST" and request.url.path != "/reset": save_world()
    return r

@app.post("/csat/{sid}/{score}")
def csat(sid: str, score: int):
    s = SESSIONS[sid]
    with LOCK: db.execute("INSERT INTO csat VALUES(?,?,?)", (datetime.now().isoformat(), sid, score)); db.commit()
    audit(s, "FOLLOWUP", f"⭐ CSAT {'👍' if score else '👎'}", {"score": score})
    reply(s, "Shukriya! 🙏 Saathi hamesha aapke saath." if score else "Maafi chahte hain — senior agent ko assign kar raha hoon.")
    if not score:
        m = MERCHANTS[s["merchant"]]; s["ctx"] = lambda tool, a: f"{m['name']} rated ticket {a['ticket']} 👎 after closure. Needs senior callback within 10 min."
        call(s, "assign_senior_callback", ticket=sid)
    return view(s)

@app.post("/lead/{sid}")
def lead(sid: str, product: str = "Business Loan", amount: int = 0):
    s = SESSIONS[sid]
    for x in MSGS:
        if x.get("cta"): x["cta_done"] = True
    if product == "EMI": return pay_emi(s)
    if product == "CAMPAIGN": return launch_campaign(s)
    if s["merchant"] in CUSTOMERS:  # customers only get EMI CTAs; "Abhi nahi" just closes it
        reply(s, "Theek hai 🙏 Jab chahein, yahin likh dijiye."); return view(s)
    MERCHANTS[s["merchant"]]["pitched"] = True
    if product == "decline":
        audit(s, "ACT", "🙅 Offer declined — will not pitch again", {})
        reply(s, "Bilkul, koi baat nahi 🙏 Jab zaroorat ho, bas 'loan' likh dijiye."); return view(s)
    with LOCK: db.execute("INSERT INTO leads VALUES(?,?,?,?)", (datetime.now().isoformat(), s["merchant"], product, amount)); db.commit()
    audit(s, "ACT", f"🎯 Lead captured: {product}", {"amount": amount})
    m = MERCHANTS[s["merchant"]]; acc = mask(m["bank"]["acc"]); oid = "ORD-" + uuid.uuid4().hex[:6].upper()
    if product == "Business Loan":
        emi = int(amount * 1.16 / 365)  # flat 16% p.a., daily from settlement
        order = {"id": oid, "item": f"Business Loan {inr(amount)}", "amount": amount, "direction": "CREDIT", "mode": f"Disbursal to bank {acc} within 24h",
                 "recurring": f"₹{emi}/day auto-deducted from daily settlement × 365 days", "status": "SANCTIONED"}
        text = f"Ho gaya! {inr(amount)} ka loan sanction ✅ Order {oid}. Paisa 24 ghante mein aapke bank {acc} mein aayega. EMI ₹{emi}/din aapke roz ke settlement se apne aap kategi — alag se kuch bharna nahi."
    else:
        order = {"id": oid, "item": "Soundbox Pro (4G)", "amount": 1, "direction": "DEBIT", "mode": "Adjusted from next settlement (no card/UPI needed)",
                 "recurring": "₹0 rental for 30 days, then ₹99/month from settlement", "status": "CONFIRMED · delivery tomorrow"}
        text = f"Ho gaya! Soundbox Pro order {oid} confirm ✅ ₹1 activation aapke agle settlement se adjust hoga (bank {acc} mein ₹1 kam aayega). 30 din free, phir ₹99/mahina settlement se. Delivery kal."
    m.setdefault("orders", []).append(order)
    audit(s, "ACT", f"💳 {order['direction']} {order['item']} · ₹{order['amount']:,} · {order['mode']}", order)
    reply(s, text)
    return view(s)

def view(s):
    return {"session_id": s["id"], "status": s["status"], "phase": s["phase"], "trace": s["trace"], "sop": s["sop"], "diff": s["diff"],
            "cache_hit": s["cache_hit"], "intent": s.get("intent"), "ml": s.get("ml"), **state(s["merchant"])}

def profile(mid="M1001"):
    if mid in CUSTOMERS:
        c = CUSTOMERS[mid]
        return {"persona": "customer", "id": mid, "name": c["name"], "lang": c["lang"], "city": c["city"], "frozen": c["frozen"], "disputes": c["disputes"],
                "phone": mask(c["phone"]), "emi": c.get("emi"), "txns": [{"id": k, **v} for k, v in c["txns"].items()],
                "passbook": ([{"kind": "DEBIT", "title": f"Paid to {v['to']}", "amount": v["amount"], "ref": "UPI " + k[-5:], "time": v["time"], "status": v["status"]} for k, v in c["txns"].items()]
                             + c.get("ledger", []))[::-1]}
    m = MERCHANTS[mid]; d = DEVICES[m["soundbox"]]; e = check_loan_and_soundbox_eligibility(mid)
    return {"id": mid, "name": m["name"], "owner": m["owner"], "gmv": m["monthly_volume"], "soundbox": m["soundbox"], "soundbox_online": d["online"],
            "settlement": "HOLD" if any(v["status"] != "SETTLED" for v in m["settlements"].values()) else "OK", "bank_verified": m["bank"]["verified"],
            "bank": mask(m["bank"]["acc"]), "offer": e, "city": m.get("city", ""),
            "txns": [{"id": k, **{f: v.get(f) for f in ("time", "payer", "amount", "status", "refunded")}} for k, v in m["txns"].items()],
            "settlements": [{"id": k, "amount": v["amount"], "date": v["date"], "status": v["status"], "utr": v.get("utr"), **{f: v[f] for f in ("gross", "fee", "gst", "count") if f in v}} for k, v in m["settlements"].items()],
            "unsettled": m.get("unsettled", []), "coupons": [coupon_view(c, detail=True) for c in m.get("coupons", {}).values()],
            "orders": m.get("orders", []), "cash": m.get("cash", []), "sb_log": m.get("sb_log", [])[::-1]}

@app.get("/state")
def state(mid: str = "M1001", agent: str = ""):
    mid = mid if mid in MERCHANTS or mid in CUSTOMERS else "M1001"
    rows = db.execute("SELECT status, auto, cache, cost, opened, resolved FROM tix").fetchall()
    res = [r for r in rows if r[0] in ("VERIFIED", "CLOSED") and r[5]]
    ttr = [r[5] - r[4] for r in res]; cache_ttr = [r[5] - r[4] for r in res if r[2]]
    avg_cost = round(sum(r[3] for r in res) / len(res), 2) if res else 0
    cs = db.execute("SELECT AVG(score), COUNT(*) FROM csat").fetchone()
    lc = db.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM leads").fetchone()
    return {"messages": [x for x in MSGS if x.get("merchant", "M1001") == mid], "profile": profile(mid),
            "merchants": [{"id": k, "name": v["name"], "owner": v["owner"], "persona": "merchant", "city": v.get("city", ""),
                           "alerts": [a for a, on in (("Settlement hold", any(x["status"] != "SETTLED" for x in v["settlements"].values())),
                                                      ("Soundbox offline", not DEVICES[v["soundbox"]]["online"]), ("Bank unverified", not v["bank"]["verified"])) if on]} for k, v in MERCHANTS.items()]
                         + [{"id": k, "name": v["name"], "owner": v["name"], "persona": "customer", "city": v.get("city", ""),
                             "alerts": [a for a, on in (("Account frozen", v["frozen"]), ("EMI unpaid", bool(v.get("emi")) and not v["emi"]["paid"]),
                                                        ("Payment pending", any(t["status"] == "PENDING" for t in v["txns"].values()))) if on]} for k, v in CUSTOMERS.items()], "cache": CACHE,
            "exec": exec_metrics(rows), "tickets": [ticket_row(x) for x in sorted(SESSIONS.values(), key=lambda x: -x["opened"])][:40],
            "agent_profile": profile(agent) if agent in MERCHANTS or agent in CUSTOMERS else None, "ml_report": ml_engine.load()["report"], "bank_status": BANK_STATUS,
            "approvals": [{k: v for k, v in a.items()} for a in APPROVALS.values() if a["status"] == "PENDING"],
            "metrics": {"tickets": len(rows), "resolved": len(res), "closed": sum(r[0] == "CLOSED" for r in rows),
                        "median_ttr_s": round(statistics.median(ttr), 1) if ttr else 0,
                        "auto_pct": round(100 * sum(r[1] for r in res) / len(res)) if res else 0,
                        "cost_per_ticket": avg_cost, "savings_pct": round(100 * (1 - avg_cost / HUMAN_COST)) if res else 0,
                        "repeat_s": round(statistics.mean(cache_ttr), 2) if cache_ttr else None, "cache_hits": sum(r[2] for r in rows),
                        "leads": lc[0], "pipeline": lc[1], "csat": round(100 * cs[0]) if cs[1] else None, "csat_n": cs[1]},
            "audit": [dict(zip(["ts", "ticket", "phase", "event", "detail", "hash"], r)) for r in db.execute("SELECT ts, ticket, phase, event, detail, hash FROM audit_chain ORDER BY id DESC LIMIT 60").fetchall()],
            "sarvam": bool(SARVAM_KEY), "live_llm": f"{LLM_PROVIDER}/{LLM_MODEL} (chain: {' → '.join(p['name'] for p in LLM_CHAIN)})" if LLM_KEY else None, "db": db.kind, "chain_ok": verify_chain(), "followup_after_s": FOLLOWUP_AFTER_S}

@app.post("/reset")
def reset():
    global LAST_HASH
    reset_world(); STAFF.clear()
    with LOCK:
        if db.pg: db.execute("TRUNCATE audit_chain, tix, leads, csat, kv")
        else:
            for op in ("update", "delete"): db.execute(f"DROP TRIGGER IF EXISTS audit_no_{op}")
            for t in ("audit_chain", "tix", "leads", "csat", "kv"): db.execute(f"DELETE FROM {t}")
            lock_audit()
        db.commit(); LAST_HASH = "GENESIS"
    return {"ok": True}

@app.get("/")
def index(): return FileResponse(os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html"))
