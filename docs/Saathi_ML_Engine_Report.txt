# Saathi ML Engine — Full Report

31 Oct 2026 · Team Confused_Devs

## Summary

Saathi ka ML engine har message pe **LLM se pehle** chalta hai, ~8 ms mein issue, action aur risk tay karta hai, aur paise wale faisle har baar same aur explainable rakhta hai. LLM ka kaam phir sirf us faisle ko user ki bhasha mein bolna hai.

- **Do models:** ek message padhke issue pehchanta hai (TF-IDF character n-grams + Logistic Regression), doosra risk features dekhke action chunta hai (Random Forest, 120 trees).
- **Data:** 1,500 synthetic Paytm cases, 7 bhashayein, 6 issue types, ek Paytm-jaisi policy se label kiye hue.
- **Held-out accuracy:** issue 100%, action 96.3% — dono synthetic data pe; asli traffic pe ye kam hogi.
- **Speed:** warm model pe ~7–9 ms per decision (pehli call ~60 ms, isliye server start pe warm-up hota hai).
- **Safety:** 60% se kam confidence pe ML koi action nahi leta; account freeze sirf tab jab user saaf fraud bole; agent kisi bhi ML faisle ko override kar sakta hai.

Files: `backend/generate_dataset.py`, `backend/ml_engine.py`, routing `backend/main.py` (`ml_route`).

## Where ML sits in the pipeline

```
Message (text / voice)
   -> ML engine (issue, action, risk)
   -> Confident (>= 60%) and right persona?
        no  -> ask the user, or the LLM agent handles it
        yes -> Guardrails (money tiers, fraud words, consent)
                 safe (AUTO)  -> Execute + verify (tools, re-check state)
                 risky        -> Human approves (Inbox or Staff WhatsApp, PIN) -> Execute + verify
   -> Reply in the user's language (LLM) + voice (Sarvam Bulbul)
```

ML sirf *propose* karta hai: kamzor ya galat-persona faisla poochhne ya LLM agent pe jaata hai, aur risky kaam (≥ ₹500, fraud, bank change) insaan ke approve karne tak rukta hai.

## Synthetic dataset

`generate_dataset.py` 1,500 rows banata hai (seed 7, isliye har baar same data) aur `backend/data/paytm_cases.csv` mein likhta hai. Asli Paytm data nahi tha, isliye ek policy likh ke usse labels banaye.

| Column | Values | Kaise banta hai |
| --- | --- | --- |
| `persona` | Customer, Merchant | Issue se tay (UPI/EMI/Fraud = Customer; baaki Merchant) |
| `lang` | Hindi, Hinglish, Tamil, Telugu, Bengali, Marathi, English | Random |
| `issue_type` | UPI_Pending, EMI_Bounce_Fear, Festival_Promo_Broadcast, Settlement_Hold, Fraud_Unauthorized_Txn, Soundbox_Sync_Issue | Random |
| `txn_amount` | ₹10 – ₹50,000 | Teen buckets: ≤₹2k, ₹2k–15k, ₹15k–50k; fraud ₹800–50k |
| `user_sentiment_score` | 0.0 (shaant) – 1.0 (ghabraya) | Gaussian; fraud ~0.85, EMI ~0.65, UPI ~0.55, baaki ~0.3 |
| `past_dispute_count` | 0 – 10 | Exponential (zyadatar 0–2) |
| `bank_server_status` | UP 70%, DEGRADED 20%, DOWN 10% | Weighted random |
| `utterance` | Asli jaisa message | Har (issue, bhasha) ke 1–7 templates + amount; sentiment > 0.7 pe ghabrahat wale shabd jod diye |
| `target_action` | 6 actions (neeche) | Policy se, 3% random noise |

**Labeling policy** (upar se neeche, pehla match jeetta hai):

1. Fraud issue, **ya** disputes ≥ 7, **ya** amount > ₹25,000 aur sentiment > 0.85 → `FREEZE_AND_HUMAN_HANDOFF`
2. UPI_Pending → amount ≤ ₹5,000 aur disputes < 5 ho toh `AUTO_REFUND_INSTANT`, warna freeze + human
3. EMI_Bounce_Fear → `SEND_EMI_LINK_WITH_REASSURANCE`
4. Festival_Promo_Broadcast → `TRIGGER_WHATSAPP_CAMPAIGN`
5. Settlement_Hold → bank DOWN nahi aur disputes < 5 ho toh `RELEASE_INSTANT_SETTLEMENT`, warna freeze + human
6. Soundbox_Sync_Issue → `REMOTE_IOT_RESTART` (maangi gayi 5 actions mein Soundbox ka koi action nahi tha, isliye ye 6th joda)

3% labels jaan-boojh ke random kiye taaki data asli ops data jaisa thoda gandaa ho aur model rules ratt na le.

## Models

Do alag models hain kyunki do alag sawal hain: *message kis baare mein hai* (text ka sawal) aur *ab kya karna safe hai* (numbers ka sawal).

| | Model 1: Issue classifier | Model 2: Action + risk |
| --- | --- | --- |
| Sawal | Message kis issue ka hai? | Is case mein kaunsa action? |
| Algorithm | TF-IDF (char_wb, 2–4 akshar n-grams, sublinear tf) + Logistic Regression (C=8, max_iter 2000) | Random Forest (120 trees, max_depth 10) |
| Input | Message ka text | 12 numbers: issue (6 one-hot) + bank status (3 one-hot) + amount/50,000 + sentiment + disputes/10 |
| Output | 6 issues ki probability | 6 actions ki probability |
| Train / test | 80% / 20% split, random_state 1 | Same |
| File | `ml_engine.py` → `train()` | Same; dono `data/models.joblib` mein save |

**Character n-grams kyun, words kyun nahi:** 7 bhashayein aur 5 lipiyan hain, aur log spelling bigaadte hain ("setelment", "paisa kata", "nai"). 2–4 akshar ke tukde Tamil, Bengali aur galat spelling sab ko ek hi tarike se pakadte hain, bina koi tokenizer ya translation ke.

**Logistic Regression kyun:** chhota, fast, aur har issue ka confidence (%) deta hai — isi confidence se Saathi tay karta hai ki action le ya poochhe.

**Random Forest kyun:** amount, disputes, bank status jaise mixed features pe rules jaisa seekhta hai ("amount > ₹5,000 toh freeze"), aur freeze ki probability se risk score banta hai.

**Model 1 ka output Model 2 ka input hai:** pehle issue, phir us issue + baaki features se action.

## Helpers and risk score

Models ke saath chaar chhote helpers chalte hain. Ye ML models nahi hain, simple rules hain, aur `decide()` inke output ko model ke saath jodta hai.

| Helper | Kaise kaam karta hai | Example |
| --- | --- | --- |
| Bhasha (`detect_lang`) | Lipi dekhta hai: Tamil, Telugu, Bengali, Devanagari. Devanagari mein "आहे, नाही, पाठवा" jaise shabd = Marathi, warna Hindi. Roman mein "hai, nahi, paisa, kar" = Hinglish, warna English | "சவுண்ட்பாக்ஸ்" → Tamil |
| Mood (`sentiment`) | 0.25 se shuru; har "!" +0.15; fraud/jaldi/darr/scared jaise shabd +0.25; CAPITALS +0.2; please/help/madad +0.1; max 1.0 | "I did NOT make this!!" → ~1.0 |
| Amount (`extract_amount`) | ₹ / Rs / INR ke baad ka number; na mile toh koi 2–6 digit number; na mile toh account se | "₹8,500 hold" → 8500 |
| Bolke amount (`spoken_amount`, Soundbox) | Digits; na hon toh "sau, hazaar, dedh sau, ढाई सौ" jaise shabd | "डेढ़ सौ रुपये" → 150 |

**Risk score (0–100):**

```
risk = min(100,  55 × P(freeze)  +  20 × sentiment  +  15 × min(amount / 50,000, 1)  +  2 × disputes)
```

P(freeze) = Random Forest ka `FREEZE_AND_HUMAN_HANDOFF` waala probability. **HIGH** = action freeze ho ya score ≥ 60; **MED** = 30–59; **LOW** < 30. Example: Rohan ka ₹12,000 fraud → HIGH 74; Ankit ka ₹420 → LOW 13.

Approval cards pe ek alag, simple risk score bhi hai (`risk_score`): bank 24h ke andar badla +35, ₹10,000+ +20, penny-drop verified −15, GMV ≥ ₹2L −5. Ye ML nahi, approver ko reasons dikhane ke liye hai.

## Routing and guardrails around the model

ML ka faisla seedha execute nahi hota: har faisla in rules se guzarta hai, aur shak ho toh Saathi poochhta hai ya insaan ko deta hai.

| Rule | Kab lagta hai | Kya hota hai |
| --- | --- | --- |
| Confidence gate | Issue confidence < 60% | ML koi action nahi leta. Merchant → LLM agent baat karta hai; customer → Saathi options deke poochhta hai |
| Persona check | ML ne customer wala issue merchant ke message pe pakda (ya ulta) | ML route band; merchant ke flows ya LLM agent sambhalta hai |
| Fraud evidence | Freeze tabhi jab user ke message mein saaf fraud shabd hon ("maine nahi kiya", fraud, stole, unauthorized, 7 bhashaon mein) | Warna "Kya hua hai?" ke options; galti se freeze kabhi nahi |
| Amount from account | Message mein amount nahi | User ke account ka amount liya jaata hai; ₹0 kabhi nahi dikhta |
| Bank change | ML ne instant settlement kaha par bank 3 din mein badla aur unverified | ML route chhod ke purana approval flow (re-settlement human approval ke saath) |
| Money tiers | Refund ≥ ₹500, instant release jab ML risk LOW nahi, fraud handoff | Human approval (Inbox, PIN 2468) |
| Merchant consent | ML ne campaign kaha | Pehle preview; merchant "Bhejo" dabaye tabhi bhejta hai |
| Agent override | Agent Tickets tab ya Staff WhatsApp se issue badalta hai | `forced_issue` → confidence 1.0, policy se naya action, audit mein agent ka naam |
| Idempotency | Dobara refund, settlement ya EMI payment | Block, "pehle hi ho chuka" |

ML ka faisla hamesha audit log mein jaata hai (issue, confidence, action, risk, bhasha, ms), aur ticket list mein wahi issue dikhta hai jis pe asal mein kaam hua (`acted` flag).

**Dhyan:** instant settlement release (ML risk LOW + KYC green) pe abhi koi amount limit nahi hai, aur customer refunds pe daily limit nahi hai. Production se pehle dono pe limit lagni chahiye.

## Accuracy, speed and tests

Numbers achhe dikhte hain, lekin ye usi synthetic data pe hain jisse model bana — asli Paytm messages pe ye kam honge.

| Metric | Value | Dhyan |
| --- | --- | --- |
| Issue accuracy (held-out 20%) | 100% | Test messages bhi unhi templates se bane hain |
| Action accuracy (held-out 20%) | 96.3% | 3% labels jaan-boojh ke random the, isliye 100% possible nahi |
| Latency, warm | ~7–9 ms; kabhi 13–15 ms | Pehli call ~60–80 ms; server start pe warm-up |
| Bhashayein | 7 | Hindi, Hinglish, Tamil, Telugu, Bengali, Marathi, English |

**Live check (asli messages):**

| Message | ML issue | Action | Risk |
| --- | --- | --- | --- |
| Maine ₹420 bheje, paisa kat gaya par dukaan wale ko nahi mila | UPI_Pending | AUTO_REFUND_INSTANT | LOW 13 |
| என் EMI கட்ட பயமா இருக்கு, late fee வருமா? | EMI_Bounce_Fear | SEND_EMI_LINK_WITH_REASSURANCE | LOW 13 |
| I didn't make this ₹12,000 payment!! someone stole my money | Fraud | FREEZE_AND_HUMAN_HANDOFF | HIGH 74 |
| Mere regular customers ko Diwali offer bhej do | Festival_Promo | TRIGGER_WHATSAPP_CAMPAIGN | LOW 5 |
| Subah se ₹8,500 ka settlement hold pe hai | Settlement_Hold | RELEASE_INSTANT_SETTLEMENT | LOW 10 |
| সকাল থেকে ₹8,500 সেটেলমেন্ট আটকে আছে (Bengali) | Settlement_Hold (97%) | RELEASE_INSTANT_SETTLEMENT | LOW 10 |
| hello boss my last transaction was failed… refund | UPI_Pending (88%) | AUTO_REFUND_INSTANT | LOW |

**Automated tests** (`backend/tests`, pytest, 44 pass): ML ke liye dataset ka shape, model quality (≥ 90%), 6 scenarios ke exact faisle, latency < 50 ms average, bhasha detection, aur "refund ko fraud mat samjho". Baaki tests poore flows check karte hain jinme ML pehla kadam hai (refund, EMI, fraud freeze, instant settlement, campaign consent, agent override, live payment).

## Bugs found and fixed

Sabse bade ML bugs ek hi tarah ke the: model ne kamzor andaze pe bada kadam utha liya. Har fix ab ek test se locked hai.

| Bug | Kya hua | Fix |
| --- | --- | --- |
| Refund ko fraud samjha | "my last transaction was failed… refund" → 64% confidence pe Fraud → account freeze, "₹0 case" | Training data mein failed/refund phrasings jode; freeze ke liye saaf fraud shabd zaroori; amount account se |
| Bina puchhe campaign | "any promos for diwali?" → 46% pe Festival_Promo → 17 customers ko WhatsApp chala gaya | Confidence gate 45% → 60%; campaign pehle preview, merchant ke "Bhejo" ke baad hi |
| Galat issue label | "349 की ट्रांजैक्शन…" ticket list mein EMI dikha, jabki ML ka faisla use hi nahi hua | `acted` flag: list mein wahi issue jis pe kaam hua |
| Proactive greeting gayab | Paytm alert ML ke instant-release raaste se gaya toh "Namaste Sunita ji…" nahi aaya | Proactive flag ML executor tak pahunchaya |
| Pehli call slow | Pehla decision ~60–80 ms | Server start pe warm-up call |

## Limitations and next steps

Sabse badi kami data hai: model ne sirf humare templates dekhe hain, isliye naye shabd ya naya andaaz aate hi wo galat ho sakta hai — isi wajah se gates aur "poochho, andaza mat lagao" rules hain.

| Kami | Asar | Production fix |
| --- | --- | --- |
| Synthetic data | 100% / 96% asli traffic pe nahi rahega | 30 din ke labelled asli tickets pe retrain; har hafte drift check |
| Templates se bani test set | Accuracy zyada dikhti hai | Test set alag logon ke likhe asli messages se |
| Sentiment = shabdon ki list | Sarcasm, gaali, naye shabd miss | Chhota multilingual sentiment model, ya LLM se score |
| Gaali / abuse ka koi alag handling nahi | Gaali wale message normal flow mein | Abuse classifier + calm reply + agent ko flag |
| 6 issue types hi | KYC, loan, QR jaise cases ML ke bahar (LLM agent sambhalta hai) | Labels badhao, ya "other" class |
| Risk formula haath se likha | Weights tune nahi kiye | Asli fraud outcomes pe calibrate (precision/recall per band) |
| Policy = rules | Model rules hi seekh raha hai | Asli agent decisions se labels, policy ko model se compare |

## How to retrain, call and test

Model file na mile toh server start pe khud train ho jaata hai (~2 sec); naya data ho toh ek command se dobara.

```
cd backend
python generate_dataset.py 1500          # CSV -> data/paytm_cases.csv
python ml_engine.py                      # train + 5 sample decisions, prints accuracy
curl -X POST localhost:8000/api/retrain  # running server: retrain in place
python -m pytest tests -q                # 44 tests, offline
```

API se seedha faisla:

```
curl -X POST localhost:8000/api/ml-decide -H "Content-Type: application/json" \
  -d '{"text":"Subah se ₹8,500 ka settlement hold pe hai","persona":"Merchant","bank":"DEGRADED"}'
# -> {issue, issue_conf, action, action_conf, risk, risk_score, lang, sentiment, amount, latency_ms}
```

| File | Kya hai |
| --- | --- |
| `backend/generate_dataset.py` | Templates, policy, generator |
| `backend/ml_engine.py` | `train`, `load`, `decide`, helpers |
| `backend/data/paytm_cases.csv` | Generated dataset (git mein) |
| `backend/data/models.joblib` | Trained models (git mein nahi; khud banta hai) |
| `backend/main.py` | `ml_route`, executors, guardrails, `/api/ml-decide`, `/api/retrain` |
| `backend/tests/test_saathi.py` | ML + flow tests |

UI mein: **Saathi Brain** tab har message ka ML faisla dikhata hai; **Executive** tab model ka naam, accuracy aur average ms.
