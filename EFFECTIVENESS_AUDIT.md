# Saathi — Effectiveness Audit (Track 3: Autonomous AI Teammate)

**Method:** every scenario was run end-to-end against the live agent (Groq `gpt-oss-120b` → Gemini fallback) with the ML engine in front. Paytm systems are **simulated**; ML is trained on **synthetic** data (issue 100%, action 98.3% held-out accuracy — expect lower on real traffic).

**Loop under test:** 01 Understands → 02 Decides & Acts → 03 Hands off on time.

## Results by point of view

| POV | Scenario | ML decision (<10ms warm) | What Saathi did | Loop closed? |
|---|---|---|---|---|
| Customer | A · ₹420 UPI debited, bank DEGRADED | `AUTO_REFUND_INSTANT` · LOW | Calmed fear in sentence 1 → verified UTR → instant refund with RRN → verified REFUNDED | ✅ same turn |
| Customer | B · EMI fear (Tamil) | `SEND_EMI_LINK_WITH_REASSURANCE` · LOW | Replied in Tamil, waived ₹250 late fee, 1-click pay → ledger shows PAID | ✅ same turn |
| Customer | C · ₹12,000 unauthorized | `FREEZE_AND_HUMAN_HANDOFF` · HIGH | Froze account, handed to risk teammate in ~20ms with 1-line summary | ✅ handed off |
| Merchant | A · "Diwali offer bhej do" | `TRIGGER_WHATSAPP_CAMPAIGN` · LOW | Segmented 90-day repeat buyers (≥3 visits), per-language offer, live conversion counter | ✅ |
| Merchant | B · ₹8,500 hold since morning | `RELEASE_INSTANT_SETTLEMENT` · LOW | Ledger + KYC green → instant release → verified UTR | ✅ |
| Merchant | Settlement after bank change | ML defers to SOP | Link sent, re-settlement waits for human approval | ✅ guarded |

## Gaps & exact fixes

### 👤 Customer
| Gap | Fix |
|---|---|
| Language replies depend on the LLM; offline fallback is Hinglish only | Pre-translate the 6 action templates into 7 languages; LLM only for free-form turns |
| Fraud freeze is account-wide | Freeze only the UPI handle/card involved; keep bill payments alive |
| No OTP before refund to a different account | Refund only to the source account (already true); add OTP for any destination change |
| Voice input not language-aware | Pass `ml.lang` to Saarika as `language_code` instead of `unknown` |

### 🏪 Merchant
| Gap | Fix |
|---|---|
| Campaign conversions are simulated over time | Wire to the real WhatsApp Business API delivery/read webhooks + Paytm payment-link callbacks |
| No consent/frequency cap check before broadcast | Filter buyers on opt-in + "max 2 promos/week" before `send_whatsapp_campaign` |
| Instant settlement threshold is hard-coded on ML risk LOW | Add a per-merchant daily instant-settlement limit and velocity check |
| Soundbox + loan flows don't use the ML engine yet | Add `REMOTE_IOT_RESTART` / loan labels to the policy and route them like the others |

### 🏢 Paytm Executive
| Gap | Fix |
|---|---|
| Synthetic training data → over-optimistic accuracy | Retrain on 30 days of labelled real tickets; track drift weekly (`/api/retrain`) |
| Loop-closure % counts tickets, not unique issues | Dedupe by customer+issue within 24h; report re-open rate alongside |
| Handoff queue has no SLA breach alerting | Page the on-call teammate if a HIGH case waits >2 min |
| Campaign GMV isn't attributed against a control group | Hold out 10% of the segment and report incremental lift, not raw GMV |
| Single approver PIN | Per-agent SSO login; approver identity already lands in the hash-chained audit log |

## Verdict
The three-step loop closes for all six scenarios. Money only moves automatically when the ML risk is LOW and the amount is small, and every high-risk case reaches a human within milliseconds with full context. The biggest remaining gap is **real data**: real integrations and retraining on real tickets.
