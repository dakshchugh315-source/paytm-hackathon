# Saathi tests

```
cd backend
pip install -r requirements.txt
python -m pytest tests -q            # offline: no LLM/Sarvam calls, isolated temp DB
RUN_LIVE=1 python -m pytest tests -q # also hits the real LLM chain + Sarvam from .env
```

Covers: ML dataset & model quality, ML decisions (6 scenarios, 7 languages), merchant flows
(settlement approval, instant release, Soundbox + cache, refunds, KYC, webhook, campaign consent,
sales pivot + orders), customer flows (UPI refund, EMI, fraud freeze, no false freeze), guardrails
(PIN, idempotency), audit chain + PII masking, restart persistence, follow-up + CSAT, agent
override/close, directory + executive metrics, voice STT, Soundbox cash entry, frontend structure + JS syntax.
