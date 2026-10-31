import os
import sys
from dotenv import load_dotenv
from sarvamai import SarvamAI

if sys.stdout.encoding != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

load_dotenv()

api_key = os.getenv("SARVAM_API_KEY")
if not api_key or api_key == "ENTER_YOUR_SARVAM_API_KEY_HERE":
    print("Error: SARVAM_API_KEY is not set in .env")
    sys.exit(1)

client = SarvamAI(api_subscription_key=api_key)

response = client.chat.completions(
    model="sarvam-105b-conversations",
    messages=[
        {"role": "user", "content": "Hello! Reply in one short friendly sentence in English and Hindi."}
    ],
)

print(response.choices[0].message.content)
