"""Synthetic Paytm support dataset: realistic multilingual utterances + structured risk features + policy label.
Run: python generate_dataset.py [rows]  ->  backend/data/paytm_cases.csv"""
import csv, os, random, sys

ISSUES = ["UPI_Pending", "EMI_Bounce_Fear", "Festival_Promo_Broadcast", "Settlement_Hold", "Fraud_Unauthorized_Txn", "Soundbox_Sync_Issue"]
LANGS = ["Hindi", "Hinglish", "Tamil", "Telugu", "Bengali", "Marathi", "English"]
ACTIONS = ["AUTO_REFUND_INSTANT", "SEND_EMI_LINK_WITH_REASSURANCE", "TRIGGER_WHATSAPP_CAMPAIGN", "RELEASE_INSTANT_SETTLEMENT", "FREEZE_AND_HUMAN_HANDOFF", "REMOTE_IOT_RESTART"]
PERSONA = {"UPI_Pending": "Customer", "EMI_Bounce_Fear": "Customer", "Fraud_Unauthorized_Txn": "Customer",
           "Festival_Promo_Broadcast": "Merchant", "Settlement_Hold": "Merchant", "Soundbox_Sync_Issue": "Merchant"}

# {amt} is filled with an amount; several phrasings per (issue, language) so the text model generalises.
T = {
 "UPI_Pending": {
  "Hindi": ["मैंने ₹{amt} भेजे, पैसा कट गया पर दुकानदार को नहीं मिला", "₹{amt} का यूपीआई पेमेंट अटका हुआ है, पैसे कटे", "पेमेंट फेल हो गया, रिफंड कर दो"],
  "Hinglish": ["Maine ₹{amt} bheje, paisa kat gaya par dukaan wale ko nahi mila", "UPI payment pending dikha raha hai, ₹{amt} debit ho gaye", "paise kat gaye lekin merchant ko credit nahi hua",
               "last payment fail ho gaya, refund kar do", "payment fail hua, paise wapas chahiye", "mera transaction fail hua refund karo please", "₹{amt} ka payment fail, refund kab milega"],
  "Tamil": ["₹{amt} அனுப்பினேன், பணம் போயிடுச்சு ஆனா கடைக்காரருக்கு வரல", "UPI பேமெண்ட் pending ஆகுது, ₹{amt} கழிஞ்சுருச்சு"],
  "Telugu": ["₹{amt} పంపాను, డబ్బు కట్ అయింది కానీ షాప్ వాళ్ళకి రాలేదు", "UPI పేమెంట్ pending లో ఉంది, ₹{amt} కట్ అయింది"],
  "Bengali": ["₹{amt} পাঠিয়েছি, টাকা কেটে গেছে কিন্তু দোকানদার পায়নি", "UPI পেমেন্ট আটকে আছে, ₹{amt} কেটেছে"],
  "Marathi": ["मी ₹{amt} पाठवले, पैसे कापले पण दुकानदाराला मिळाले नाहीत", "UPI पेमेंट अडकले आहे, ₹{amt} कापले गेले"],
  "English": ["I sent ₹{amt}, money got debited but the shop didn't receive it", "UPI payment stuck in pending, ₹{amt} deducted",
              "my last transaction failed, please refund it", "payment failed but money deducted, need a refund", "transaction failed, refund my money", "hi my payment of ₹{amt} failed can you refund"]},
 "EMI_Bounce_Fear": {
  "Hindi": ["मेरी ईएमआई बाउंस हो जाएगी, लेट फीस लगेगी क्या?", "₹{amt} की किस्त भरने में डर लग रहा है"],
  "Hinglish": ["Meri EMI bounce ho jayegi kya, late fee lagegi?", "₹{amt} ki kisht bharni hai, darr lag raha hai penalty ka", "loan EMI miss ho gayi, ab kya hoga"],
  "Tamil": ["என் EMI கட்ட பயமா இருக்கு, late fee வருமா?", "₹{amt} EMI bounce ஆயிடுமா?"],
  "Telugu": ["నా EMI bounce అవుతుందా, late fee పడుతుందా?", "₹{amt} EMI కట్టాలి, భయంగా ఉంది"],
  "Bengali": ["আমার EMI বাউন্স হবে না তো, লেট ফি লাগবে?", "₹{amt} কিস্তি দিতে ভয় লাগছে"],
  "Marathi": ["माझा EMI बाउन्स होईल का, लेट फी लागेल का?", "₹{amt} चा हप्ता भरायची भीती वाटते"],
  "English": ["Will my EMI bounce? I'm worried about the late fee", "Scared I'll miss my ₹{amt} loan installment"]},
 "Festival_Promo_Broadcast": {
  "Hindi": ["मेरे रेगुलर ग्राहकों को दिवाली ऑफर भेज दो", "त्योहार का ऑफर सब ग्राहकों को भेजना है"],
  "Hinglish": ["Mere regular customers ko Diwali offer bhej do", "festival offer sab customers ko WhatsApp kar do", "purane grahakon ko discount message bhejo"],
  "Tamil": ["என் வழக்கமான வாடிக்கையாளர்களுக்கு தீபாவளி ஆஃபர் அனுப்புங்க"],
  "Telugu": ["నా రెగ్యులర్ కస్టమర్లకి దీపావళి ఆఫర్ పంపండి"],
  "Bengali": ["আমার নিয়মিত খদ্দেরদের দিওয়ালি অফার পাঠিয়ে দিন"],
  "Marathi": ["माझ्या नेहमीच्या ग्राहकांना दिवाळी ऑफर पाठवा"],
  "English": ["Send a Diwali offer to my regular customers", "Broadcast a festive discount to repeat buyers"]},
 "Settlement_Hold": {
  "Hindi": ["सुबह से ₹{amt} का सेटलमेंट होल्ड पर है", "कल का सेटलमेंट नहीं आया"],
  "Hinglish": ["Subah se ₹{amt} ka settlement hold pe hai", "Kal ka settlement nahi aaya", "payout ruka hua hai, ₹{amt} abhi tak nahi aaye"],
  "Tamil": ["காலையிலிருந்து ₹{amt} settlement hold ஆகியிருக்கு"],
  "Telugu": ["పొద్దున్నుంచి ₹{amt} settlement hold లో ఉంది"],
  "Bengali": ["সকাল থেকে ₹{amt} সেটেলমেন্ট আটকে আছে"],
  "Marathi": ["सकाळपासून ₹{amt} चे सेटलमेंट होल्डवर आहे"],
  "English": ["My ₹{amt} settlement has been on hold since morning", "Yesterday's settlement didn't arrive"]},
 "Fraud_Unauthorized_Txn": {
  "Hindi": ["मैंने ये ₹{amt} का पेमेंट नहीं किया, किसी ने पैसे निकाल लिए", "मेरे खाते से बिना बताए ₹{amt} कट गए"],
  "Hinglish": ["Maine ye ₹{amt} ka payment nahi kiya, fraud hua hai", "kisi ne mere account se ₹{amt} nikaal liye", "unknown transaction aaya hai, mera paisa chori ho gaya"],
  "Tamil": ["நான் இந்த ₹{amt} பேமெண்ட் பண்ணல, யாரோ பணம் எடுத்துட்டாங்க"],
  "Telugu": ["నేను ఈ ₹{amt} పేమెంట్ చేయలేదు, ఎవరో డబ్బు తీసుకున్నారు"],
  "Bengali": ["আমি এই ₹{amt} পেমেন্ট করিনি, কেউ টাকা তুলে নিয়েছে"],
  "Marathi": ["मी हे ₹{amt} पेमेंट केले नाही, कोणीतरी पैसे काढले"],
  "English": ["I didn't make this ₹{amt} payment, someone stole my money", "Unauthorized transaction of ₹{amt} on my account",
              "someone used my account without permission", "I don't recognise this ₹{amt} transaction, it's fraud"]},
 "Soundbox_Sync_Issue": {
  "Hindi": ["साउंडबॉक्स बोल नहीं रहा", "पेमेंट की आवाज़ नहीं आ रही"],
  "Hinglish": ["Soundbox bol nahi raha", "payment ki awaaz nahi aa rahi soundbox se", "soundbox offline dikha raha hai"],
  "Tamil": ["சவுண்ட்பாக்ஸ் பேசல"],
  "Telugu": ["సౌండ్‌బాక్స్ మాట్లాడట్లేదు"],
  "Bengali": ["সাউন্ডবক্স আওয়াজ করছে না"],
  "Marathi": ["साउंडबॉक्स बोलत नाही"],
  "English": ["Soundbox is silent, no payment announcements"]},
}
PANIC = {"Hinglish": [" jaldi karo!!", " please help", " bahut pareshan hoon"], "English": [" please help!!", " urgent", " I'm panicking"],
         "Hindi": [" जल्दी करो!!", " बहुत परेशान हूँ"], "Tamil": [" சீக்கிரம்!!"], "Telugu": [" త్వరగా!!"], "Bengali": [" তাড়াতাড়ি!!"], "Marathi": [" लवकर!!"]}

def policy(issue, amt, senti, disputes, bank):
    """Paytm-style decision policy the model learns (ground truth)."""
    if issue == "Fraud_Unauthorized_Txn" or disputes >= 7 or (amt > 25000 and senti > 0.85): return "FREEZE_AND_HUMAN_HANDOFF"
    if issue == "UPI_Pending": return "AUTO_REFUND_INSTANT" if amt <= 5000 and disputes < 5 else "FREEZE_AND_HUMAN_HANDOFF"
    if issue == "EMI_Bounce_Fear": return "SEND_EMI_LINK_WITH_REASSURANCE"
    if issue == "Festival_Promo_Broadcast": return "TRIGGER_WHATSAPP_CAMPAIGN"
    if issue == "Settlement_Hold": return "RELEASE_INSTANT_SETTLEMENT" if bank != "DOWN" and disputes < 5 else "FREEZE_AND_HUMAN_HANDOFF"
    return "REMOTE_IOT_RESTART"

def row(rng):
    issue, lang = rng.choice(ISSUES), rng.choice(LANGS)
    amt = int(rng.choice([rng.randint(10, 2000), rng.randint(2000, 15000), rng.randint(15000, 50000)]))
    if issue == "Fraud_Unauthorized_Txn": amt = rng.randint(800, 50000)
    senti = round(min(1.0, max(0.0, rng.gauss({"Fraud_Unauthorized_Txn": 0.85, "UPI_Pending": 0.55, "EMI_Bounce_Fear": 0.65}.get(issue, 0.3), 0.15))), 2)
    disputes = min(10, int(rng.expovariate(0.6)))
    bank = rng.choices(["UP", "DEGRADED", "DOWN"], [0.7, 0.2, 0.1])[0]
    text = rng.choice(T[issue][lang]).format(amt=f"{amt:,}")
    if senti > 0.7: text += rng.choice(PANIC[lang])
    label = policy(issue, amt, senti, disputes, bank)
    if rng.random() < 0.03: label = rng.choice(ACTIONS)  # 3% label noise, like real ops data
    return {"persona": PERSONA[issue], "lang": lang, "issue_type": issue, "txn_amount": amt, "user_sentiment_score": senti,
            "past_dispute_count": disputes, "bank_server_status": bank, "utterance": text, "target_action": label}

def generate(n=1500, seed=7):
    rng = random.Random(seed); return [row(rng) for _ in range(n)]

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1500
    rows = generate(n); out = os.path.join(os.path.dirname(__file__), "data", "paytm_cases.csv"); os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    print(f"wrote {n} rows -> {out}")
