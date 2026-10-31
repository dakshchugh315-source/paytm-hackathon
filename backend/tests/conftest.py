"""Test harness: isolated SQLite file, no network (LLM, Sarvam, edge-tts all off), fresh world per test.
Live-LLM checks run only with RUN_LIVE=1."""
import os, sys, tempfile
import pytest

_tmp = tempfile.mkdtemp(prefix="saathi-test-")
os.environ["SAATHI_SQLITE"] = os.path.join(_tmp, "test.db")
for k in ("GROQ_API_KEY", "GEMINI_API_KEY", "GROK_API_KEY", "OPENAI_API_KEY", "SARVAM_API_KEY"):
    os.environ[k] = os.environ.get(k, "") if os.getenv("RUN_LIVE") else ""  # setdefault in main keeps these empty
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

PIN = {"x-approver-pin": main.APPROVER_PIN}

@pytest.fixture
def c(monkeypatch):
    if not os.getenv("RUN_LIVE"):
        monkeypatch.setattr(main, "LLM_KEY", False); monkeypatch.setattr(main, "LLM_CHAIN", []); monkeypatch.setattr(main, "SARVAM_KEY", "")
        monkeypatch.setattr(main, "tts", lambda text: {"source": "browser"})
    with TestClient(main.app) as client:
        client.post("/reset")
        yield client

def chat(c, text, uid="M1001", sid=None):
    return c.post("/chat", json={"text": text, "merchant_id": uid, "session_id": sid}).json()

def approve(c, r, tool=None):
    ap = next(a for a in r["approvals"] if tool is None or a["tool"] == tool)
    return c.post(f"/approvals/{ap['id']}/approve", headers=PIN).json()
