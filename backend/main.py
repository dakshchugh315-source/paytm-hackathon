"""Saathi — autonomous support & sales teammate for Paytm merchants (Sarvam AI). Team Confused_Devs.
Run: pip install fastapi uvicorn httpx python-multipart && uvicorn main:app --reload
Env: SARVAM_API_KEY (optional), SAATHI_DB_URL=postgresql://... (optional; SQLite otherwise),
     APPROVER_PIN (default 2468), FOLLOWUP_AFTER_S (default 86400 = real 24h scheduler)."""
import os, re, json, time, uuid, sqlite3, hashlib, base64, math, struct, threading, statistics, copy
from datetime import datetime
import httpx, asyncio
try: import edge_tts
except ImportError: edge_tts = None
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

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
        if not self.pg: self.c = sqlite3.connect(os.path.join(os.path.dirname(__file__), "saathi.db"), check_same_thread=False)
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
              "settlements": {"ST-9102": {"amount": 42300, "date": "yesterday", "status": "SETTLED", "reason": "", "utr": "UTR8821AB09"}},
              "txns": {"430722310045": {"amount": 2450, "time": "Today 11:05", "payer": "dr.mehta@icici", "status": "FAILED", "debited": True, "refunded": False},
                       "430722310031": {"amount": 180, "time": "Today 10:20", "payer": "neha.v@okhdfc", "status": "SUCCESS", "debited": True, "refunded": False}}},
    "M1003": {"name": "Raju Chai Stall", "owner": "Raju", "city": "Lucknow", "phone": "9900112233", "monthly_volume": 45000, "has_loan": False, "soundbox": "SB-9102",
              "bank": {"acc": "12340098765432", "ifsc": "PUNB0123400", "changed_days_ago": 400, "verified": True},
              "settlements": {"ST-9203": {"amount": 1520, "date": "yesterday", "status": "SETTLED", "reason": "", "utr": "UTR7710CD12"}},
              "txns": {"430733401120": {"amount": 20, "time": "Today 08:02", "payer": "student21@ybl", "status": "FAILED", "debited": True, "refunded": False}}},
}

def reset_world():
    global MERCHANTS, DEVICES, SESSIONS, APPROVALS, CACHE, MSGS
    MERCHANTS = copy.deepcopy(SEED); DEVICES = {"SB-7781": {"online": False, "fault": "network", "last_ping": "14 min ago"}, "SB-8820": {"online": True, "fault": None, "last_ping": "just now"},
               "SB-9102": {"online": False, "fault": "network", "last_ping": "2 h ago"}}
    SESSIONS, APPROVALS, CACHE, MSGS = {}, {}, {}, []
reset_world()

def save_world():
    msgs = [{**{k: v for k, v in m.items() if k != "voice"}, **({"voice": {"source": "browser"}} if m.get("voice") else {})} for m in MSGS]
    with LOCK:
        db.execute("INSERT INTO kv VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", ("world", json.dumps({"m": MERCHANTS, "d": DEVICES, "c": CACHE, "msgs": msgs}, ensure_ascii=False))); db.commit()

def load_world():
    global MERCHANTS, DEVICES, CACHE, MSGS
    r = db.execute("SELECT v FROM kv WHERE k='world'").fetchone()
    if r: w = json.loads(r[0]); MERCHANTS, DEVICES, CACHE, MSGS = w["m"], w["d"], w["c"], w["msgs"]

# ---------------- PII masking + immutable audit ----------------
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
            r = httpx.post(f"{SARVAM}/text-to-speech", headers=_h(), timeout=20, json={"inputs": [text[:500]], "target_language_code": lang, "speaker": spk, "model": "bulbul:v2"})
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
            audio = asyncio.run(_gen())
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
                          "summary": s["ctx"](tool, args), "created": time.time()}
        s["status"] = "WAITING_APPROVAL"; s["auto"] = False
        audit(s, "HUMAN", f"⏸ {tool} → NEEDS_APPROVAL ({aid})", {"tool": tool, "args": args, "tier": t, "reason": why}); return None
    res = TOOLS[tool](s["merchant"], **args); s["tools"] += 1
    audit(s, "ACT", f"✔ {tool} [AUTO]", {"tool": tool, "args": args, "tier": t, "result": res}); return res

def snapshot(mid):
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
    if s["intent"] != "sales" and not MERCHANTS[s["merchant"]].get("pitched") and calm(s): MERCHANTS[s["merchant"]]["pitched"] = True; pivot(s)

def calm(s):
    """Pitch only at a quiet moment: no other open ticket, pending approval or unanswered question for this merchant."""
    busy = [x for x in SESSIONS.values() if x is not s and x["merchant"] == s["merchant"] and x["status"] in ("OPEN", "DIAGNOSING", "WAITING_APPROVAL", "EXECUTING", "NEEDS_INFO")]
    return not busy and not any(a["status"] == "PENDING" for a in APPROVALS.values())

def pivot(s):
    """Support-to-Sales: pitch at the high-CSAT moment right after a verified fix."""
    o = TOOLS["check_loan_and_soundbox_eligibility"](s["merchant"])
    audit(s, "DIAGNOSE", f"💰 Sales pivot: {o['product']}", o)
    if o["product"] == "Business Loan":
        reply(s, f"Waise, aapke store ki badhti bikri dekhte hue aap ₹{o['pre_approved']//100000} lakh ke Paytm Business Loan ke liye pre-approved hain, sirf ₹{o['emi_daily']}/din. 1-click mein apply kar doon?",
              cta={"label": "✅ Haan, apply karo", "product": o["product"], "amount": o["pre_approved"]}, kind="pitch")
    else:
        reply(s, f"Waise, Soundbox Pro {o['offer']} pe mil raha hai. Upgrade kar doon?", cta={"label": "✅ Haan, upgrade karo", "product": o["product"], "amount": 0}, kind="pitch")

def signature(intent, ctx): return f"{intent}:{ctx}"

def run_agent(s, text, proactive=False, live=True):
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
            reply(s, "Mujhe ye failed payments mile. Kaunsa wala?", options=[f"{t['time'].replace('Today ', '')} wala ₹{t['amount']:,} ({t['payer'][:3]}…) — ref ...{k[-4:]}" for k, t in cands.items()]); return
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
- NEVER ask the merchant for data you can fetch. Merchants don't know txn IDs: call lookup_transaction first, then identify the payment by amount/time/payer (e.g. "10:32 wala ₹349, rahul.k@… se?").
- If the merchant gives an ID/number that doesn't match, say it wasn't found and list the matching recent payments (time, amount, masked payer, last 4 of ref).
- If several transactions could match, ask which one; act only on the one the merchant means.
- Some tools need human approval; if a tool returns PENDING_HUMAN_APPROVAL, tell the merchant it is with a senior for approval. Never claim it is done.
- A message starting with EVENT is a Paytm backend alert: message the merchant first, proactively, by name.
- Do not pitch loans/offers unless the merchant asks.
- Address the merchant by first name + "ji" only in the first reply of a conversation, not every message.
- Multi-language support: Reply in the SAME language and script as the merchant's query. If English is used, reply in concise natural English. If Hinglish (Roman script) is used, reply in warm Hinglish. If pure Hindi (Devanagari) or another Indic language is used, reply in that language. Under 40 words, stating exactly what you did and verified. Mask account/phone numbers as XXXX-1234.
Merchant: {m['name']} (owner {m['owner']}, {m.get('city', '')}), id {s['merchant']}, Soundbox {m['soundbox']}, GMV Rs {m['monthly_volume']}/month.
Account context (live): recent payments {json.dumps([{"ref": k, "time": v["time"], "amount": v["amount"], "payer": v["payer"][:3] + "…", "status": v["status"], "refunded": v["refunded"]} for k, v in m["txns"].items()], ensure_ascii=False)};
settlements {json.dumps({k: {"amount": v["amount"], "status": v["status"]} for k, v in m["settlements"].items()})}; Soundbox online={DEVICES[m["soundbox"]]["online"]}.
Mention refs to the merchant only as last 4 digits. Use this context to propose the likely payment right away (e.g. "10:32 wala ₹349?") instead of asking open questions; still call tools to act and to verify.
Retrieved SOP: {json.dumps(sop, ensure_ascii=False) if sop else 'none'}
{('Verified fix from a past ticket (reuse it, skip re-diagnosis): ' + ' -> '.join(cached['plan'])) if cached else ''}"""

def llm_chat(msgs):
    """Try providers in order; a 429/quota/5xx/timeout puts that provider on cooldown and falls through to the next."""
    global LLM_PROVIDER, LLM_MODEL
    errs = []
    for p in LLM_CHAIN + LLM_CHAIN:  # second pass = one retry per provider (transient 400/503)
        if p["cool_until"] > time.time(): continue
        try:
            r = httpx.post(f"{p['base']}/chat/completions", headers={"Authorization": f"Bearer {p['key']}", "api-subscription-key": p['key']}, timeout=40,
                           json={"model": p["model"], "messages": msgs, "tools": LLM_TOOLS, "temperature": 0.2})
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
    s["llm_msgs"] = [{"role": "system", "content": llm_system(s, s["sop"], cached)}, *hist, {"role": "user", "content": text}]
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
    MSGS.append({"from": "merchant", "text": ("🎤 " if voice_in else "") + text, "ts": time.time(), "merchant": s["merchant"]}); run_agent(s, text); return view(s)

@app.post("/chat")
def chat(m: Msg):
    if m.merchant_id not in MERCHANTS: raise HTTPException(404, "merchant")
    return handle(session(m.session_id, m.merchant_id), m.text.replace("🎤", "").strip(), m.text.startswith("🎤"))

@app.post("/voice")
async def voice(file: UploadFile = File(...), merchant_id: str = Form("M1001"), hint: str = Form("")):
    if merchant_id not in MERCHANTS: raise HTTPException(404, "merchant")
    text = stt(await file.read(), file.filename) or hint or "Kal ka settlement nahi aaya"
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
    load_world(); threading.Thread(target=scheduler, daemon=True).start()

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
    MERCHANTS[s["merchant"]]["pitched"] = True
    if product == "decline":
        audit(s, "ACT", "🙅 Offer declined — will not pitch again", {})
        reply(s, "Bilkul, koi baat nahi 🙏 Jab zaroorat ho, bas 'loan' likh dijiye."); return view(s)
    with LOCK: db.execute("INSERT INTO leads VALUES(?,?,?,?)", (datetime.now().isoformat(), s["merchant"], product, amount)); db.commit()
    audit(s, "ACT", f"🎯 Lead captured: {product}", {"amount": amount})
    reply(s, f"Ho gaya! {product} application submit kar di ✅ Paise 24 ghante mein account mein." if product == "Business Loan" else "Ho gaya! Soundbox Pro kal deliver hoga ✅")
    return view(s)

def view(s):
    return {"session_id": s["id"], "status": s["status"], "phase": s["phase"], "trace": s["trace"], "sop": s["sop"], "diff": s["diff"],
            "cache_hit": s["cache_hit"], "intent": s.get("intent"), **state(s["merchant"])}

def profile(mid="M1001"):
    m = MERCHANTS[mid]; d = DEVICES[m["soundbox"]]; e = check_loan_and_soundbox_eligibility(mid)
    return {"id": mid, "name": m["name"], "owner": m["owner"], "gmv": m["monthly_volume"], "soundbox": m["soundbox"], "soundbox_online": d["online"],
            "settlement": "HOLD" if any(v["status"] != "SETTLED" for v in m["settlements"].values()) else "OK", "bank_verified": m["bank"]["verified"],
            "bank": mask(m["bank"]["acc"]), "offer": e, "city": m.get("city", ""),
            "txns": [{"id": k, **{f: v.get(f) for f in ("time", "payer", "amount", "status", "refunded")}} for k, v in m["txns"].items()],
            "settlements": [{"id": k, "amount": v["amount"], "date": v["date"], "status": v["status"], "utr": v.get("utr")} for k, v in m["settlements"].items()]}

@app.get("/state")
def state(mid: str = "M1001"):
    mid = mid if mid in MERCHANTS else "M1001"
    rows = db.execute("SELECT status, auto, cache, cost, opened, resolved FROM tix").fetchall()
    res = [r for r in rows if r[0] in ("VERIFIED", "CLOSED")]
    ttr = [r[5] - r[4] for r in res]; cache_ttr = [r[5] - r[4] for r in res if r[2]]
    avg_cost = round(sum(r[3] for r in res) / len(res), 2) if res else 0
    cs = db.execute("SELECT AVG(score), COUNT(*) FROM csat").fetchone()
    lc = db.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM leads").fetchone()
    return {"messages": [x for x in MSGS if x.get("merchant", "M1001") == mid], "profile": profile(mid),
            "merchants": [{"id": k, "name": v["name"], "owner": v["owner"]} for k, v in MERCHANTS.items()], "cache": CACHE,
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
    reset_world()
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
