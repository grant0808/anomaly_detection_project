import json
import os
from pathlib import Path

import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from model import DeepLogLSTM
from preprocess import EventIndexer


class PredictRequest(BaseModel):
    instances: list[list[int]]
    top_g: int | None = None


class TraceRequest(BaseModel):
    sequence: list[int]
    window_size: int | None = None
    top_g: int | None = None


app = FastAPI(title="DeepLog KServe Predictor", version="1.0.0")

MODEL_NAME = os.getenv("MODEL_NAME", "deeplog")
MODEL_DIR = Path(os.getenv("MODEL_DIR", "/mnt/models"))
VOCAB_PATH = Path(os.getenv("VOCAB_PATH", MODEL_DIR / "vocab.json"))
WEIGHTS_PATH = Path(os.getenv("WEIGHTS_PATH", MODEL_DIR / "deeplog_lstm.pth"))
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "10"))
TOP_G = int(os.getenv("TOP_G", "9"))
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "64"))
HIDDEN_SIZE = int(os.getenv("HIDDEN_SIZE", "64"))
NUM_LAYERS = int(os.getenv("NUM_LAYERS", "2"))
DEVICE = os.getenv("DEVICE", "cuda" if torch.cuda.is_available() else "cpu")

model: DeepLogLSTM | None = None
indexer: EventIndexer | None = None


def load_artifacts() -> None:
    global model, indexer

    if not VOCAB_PATH.exists():
        raise FileNotFoundError(f"Vocab file not found: {VOCAB_PATH}")
    if not WEIGHTS_PATH.exists():
        raise FileNotFoundError(f"Model weights not found: {WEIGHTS_PATH}")

    indexer = EventIndexer.load(str(VOCAB_PATH))
    model = DeepLogLSTM(
        num_classes=indexer.num_events,
        embedding_dim=EMBEDDING_DIM,
        hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS,
    ).to(DEVICE)
    state_dict = torch.load(WEIGHTS_PATH, map_location=DEVICE)
    model.load_state_dict(state_dict)
    model.eval()


@app.on_event("startup")
def startup() -> None:
    load_artifacts()


@app.get("/v1/models/{model_name}")
def model_ready(model_name: str):
    if model_name != MODEL_NAME:
        raise HTTPException(status_code=404, detail="Unknown model")
    return {"name": MODEL_NAME, "ready": model is not None}


@app.post("/v1/models/{model_name}:predict")
def predict(model_name: str, request: PredictRequest):
    if model_name != MODEL_NAME:
        raise HTTPException(status_code=404, detail="Unknown model")
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded")
    if not request.instances:
        return {"predictions": []}

    top_g = request.top_g or TOP_G
    tensor = torch.tensor(request.instances, dtype=torch.long).to(DEVICE)
    with torch.no_grad():
        logits = model(tensor)
        probs = torch.softmax(logits, dim=-1)
        top_probs, top_indices = torch.topk(probs, top_g, dim=-1)

    predictions = []
    for indices, scores in zip(top_indices.cpu().tolist(), top_probs.cpu().tolist()):
        predictions.append(
            {
                "top_indices": indices,
                "top_scores": scores,
            }
        )
    return {"predictions": predictions}


@app.post("/v1/models/{model_name}:detect")
def detect(model_name: str, request: TraceRequest):
    if model_name != MODEL_NAME:
        raise HTTPException(status_code=404, detail="Unknown model")
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded")

    result = model.detect_anomalies_in_trace(
        request.sequence,
        window_size=request.window_size or WINDOW_SIZE,
        top_g=request.top_g or TOP_G,
        device=DEVICE,
    )
    return result


@app.get("/healthz")
def healthz():
    return {
        "status": "healthy",
        "model_name": MODEL_NAME,
        "model_loaded": model is not None,
        "model_dir": str(MODEL_DIR),
    }


@app.get("/metadata")
def metadata():
    if indexer is None:
        raise HTTPException(status_code=503, detail="Vocab is not loaded")
    return {
        "model_name": MODEL_NAME,
        "num_events": indexer.num_events,
        "window_size": WINDOW_SIZE,
        "top_g": TOP_G,
    }
