"""ml_server.py — FastAPI-server för V8 multivariat Isolation Forest.

Pi-arkitekturen (V8):
  - rssi_logger (AppDaemon) skriver CSV till /share/data/
  - inference_app_v8 (AppDaemon):
      * Tail:ar CSV var 10:e sekund
      * Beräknar 8-dim delta-feature-vektor (4 ankares median + 4 std)
      * Anropar denna server för IF-prediktion
      * Kör persistens + period-extraktion lokalt
  - Denna server lyssnar på port 8765
      * Laddar if_model.joblib från /app/models/
      * Returnerar single decision_function-score för 8-dim feature-vektor

V7-arkitekturen (per-anchor IF + weighted score) är avvecklad.
"""
from pathlib import Path
from typing import Dict, List

import joblib
import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel

MODEL_PATH = Path("/app/models/if_model.joblib")

app = FastAPI(title="IoT-Projekt V8 multivariate IF", version="8.0")


# ---------- Modell-laddning ----------
_bundle = None


def _ensure_loaded():
    global _bundle
    if _bundle is None:
        if not MODEL_PATH.exists():
            raise RuntimeError(f"Model bundle not found at {MODEL_PATH}")
        _bundle = joblib.load(MODEL_PATH)
    return _bundle


# ---------- Request/response-scheman ----------
class PredictRequest(BaseModel):
    """8-dim feature-vektor: median-delta + std-delta per primär-ankare."""
    features: List[float]


class PredictResponse(BaseModel):
    score: float                  # decision_function (mer negativt = mer anomalt)
    is_anomaly: bool              # score < score_threshold
    score_threshold: float        # tröskeln modellen tränades med


# ---------- Endpoints ----------
@app.get("/health")
def health():
    bundle = _ensure_loaded()
    return {
        "status": "ok",
        "version": bundle.get("version", "unknown"),
        "primary_anchors": bundle.get("primary_anchors", []),
        "feature_cols": bundle.get("feature_cols", []),
        "score_threshold": bundle.get("score_threshold"),
        "persistence_s": bundle.get("persistence_s"),
    }


@app.get("/info")
def info():
    bundle = _ensure_loaded()
    return {
        "version": bundle.get("version", "unknown"),
        "trained_at": bundle.get("trained_at"),
        "strategy": bundle.get("strategy"),
        "primary_anchors": bundle.get("primary_anchors", []),
        "feature_cols": bundle.get("feature_cols", []),
        "baseline_init": bundle.get("baseline_init", {}),
        "baseline_std_init": bundle.get("baseline_std_init", {}),
        "ewma_halflife_min": bundle.get("ewma_halflife_min", 30),
        "score_threshold": bundle.get("score_threshold"),
        "persistence_s": bundle.get("persistence_s"),
        "params": bundle.get("params", {}),
    }


@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    bundle = _ensure_loaded()
    expected_dim = len(bundle.get("feature_cols", []))

    if len(req.features) != expected_dim:
        return PredictResponse(
            score=0.0, is_anomaly=False,
            score_threshold=float(bundle["score_threshold"]),
        )

    x = np.array(req.features, dtype=float).reshape(1, -1)
    score = float(bundle["model"].decision_function(x)[0])
    thr = float(bundle["score_threshold"])
    return PredictResponse(
        score=score,
        is_anomaly=score < thr,
        score_threshold=thr,
    )
