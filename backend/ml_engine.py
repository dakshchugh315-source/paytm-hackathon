"""ML decision engine (runs before the LLM): issue classifier on text + action policy model on risk features.
Deterministic, <10ms per decision; the LLM only phrases the chosen action in the user's language."""
import os, re, time, joblib
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import train_test_split
from generate_dataset import generate, policy, ISSUES, PERSONA

MODEL_PATH = os.path.join(os.path.dirname(__file__), "data", "models.joblib")
BANKS = ["UP", "DEGRADED", "DOWN"]
_M = {}

def _vec(issue, amt, senti, disputes, bank):
    return [*(float(issue == i) for i in ISSUES), *(float(bank == b) for b in BANKS), amt / 50000, senti, disputes / 10]

def train(n=1500):
    rows = generate(n)
    txt_tr, txt_te, iss_tr, iss_te = train_test_split([r["utterance"] for r in rows], [r["issue_type"] for r in rows], test_size=0.2, random_state=1)
    clf = make_pipeline(TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4), sublinear_tf=True), LogisticRegression(max_iter=2000, C=8))
    clf.fit(txt_tr, iss_tr)
    X = [_vec(r["issue_type"], r["txn_amount"], r["user_sentiment_score"], r["past_dispute_count"], r["bank_server_status"]) for r in rows]
    y = [r["target_action"] for r in rows]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=1)
    rf = RandomForestClassifier(n_estimators=120, max_depth=10, random_state=1).fit(X_tr, y_tr)
    report = {"rows": n, "issue_acc": round(clf.score(txt_te, iss_te), 3), "action_acc": round(rf.score(X_te, y_te), 3),
              "models": "TF-IDF char n-gram + LogisticRegression (issue) · RandomForest (action)"}
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True); joblib.dump({"clf": clf, "rf": rf, "report": report}, MODEL_PATH)
    return report

def load():
    if not _M:
        if not os.path.exists(MODEL_PATH): train()
        _M.update(joblib.load(MODEL_PATH))
    return _M

def detect_lang(t):
    if re.search(r"[஀-௿]", t): return "Tamil"
    if re.search(r"[ఀ-౿]", t): return "Telugu"
    if re.search(r"[ঀ-৿]", t): return "Bengali"
    if re.search(r"[ऀ-ॿ]", t): return "Marathi" if re.search(r"आहे|नाही|पाठवा|माझ|मी |काढले|मिळाले", t) else "Hindi"
    return "Hinglish" if re.search(r"\b(hai|nahi|kya|mera|mere|maine|paisa|paise|bhej|kar|ho|gaya|raha|kal|ka|ki)\b", t, re.I) else "English"

def sentiment(t):
    s = 0.25 + 0.15 * t.count("!") + 0.25 * bool(re.search(r"fraud|chori|stole|unauthori|panic|jaldi|urgent|pareshan|darr|dar |scared|worried|भीती|डर|பயம|భయం|ভয়|चोरी|திருட|నేను ఈ|তুলে", t, re.I))
    s += 0.2 * bool(re.search(r"\b[A-Z]{4,}\b", t)) + 0.1 * bool(re.search(r"please|help|madad", t, re.I))
    return round(min(1.0, s), 2)

def extract_amount(t):
    m = re.search(r"(?:₹|rs\.?\s?|inr\s?)\s?([\d,]+)", t, re.I) or re.search(r"\b(\d{2,6})\b", t.replace(",", ""))
    return int(m.group(1).replace(",", "")) if m else None

def decide(text, persona="Customer", amount=None, disputes=0, bank="UP"):
    t0 = time.perf_counter(); m = load()
    probs = m["clf"].predict_proba([text])[0]; i = int(np.argmax(probs)); issue = m["clf"].classes_[i]
    amt = extract_amount(text) or amount or 0; senti = sentiment(text)
    ap = m["rf"].predict_proba([_vec(issue, amt, senti, disputes, bank)])[0]; j = int(np.argmax(ap)); action = m["rf"].classes_[j]
    p_freeze = float(ap[list(m["rf"].classes_).index("FREEZE_AND_HUMAN_HANDOFF")]) if "FREEZE_AND_HUMAN_HANDOFF" in m["rf"].classes_ else 0
    score = round(min(100, 55 * p_freeze + 20 * senti + 15 * min(amt / 50000, 1) + 2 * disputes))
    risk = "HIGH" if action == "FREEZE_AND_HUMAN_HANDOFF" or score >= 60 else "MED" if score >= 30 else "LOW"
    return {"issue": issue, "issue_conf": round(float(probs[i]), 2), "action": action, "action_conf": round(float(ap[j]), 2),
            "risk": risk, "risk_score": score, "lang": detect_lang(text), "sentiment": senti, "amount": amt,
            "persona_expected": PERSONA[issue], "persona": persona, "bank_status": bank, "disputes": disputes,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 2)}

if __name__ == "__main__":
    print(train())
    for q in ["Maine ₹420 bheje, paisa kat gaya par dukaan wale ko nahi mila", "என் EMI கட்ட பயமா இருக்கு, late fee வருமா?",
              "I didn't make this ₹12,000 payment!! someone stole my money", "Mere regular customers ko Diwali offer bhej do", "Subah se ₹8,500 ka settlement hold pe hai"]:
        d = decide(q, bank="DEGRADED"); print(d["lang"], d["issue"], d["action"], d["risk"], d["risk_score"], f'{d["latency_ms"]}ms')
