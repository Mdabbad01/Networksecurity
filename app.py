import os
import sys
import certifi
import pandas as pd
import pymongo
from dotenv import load_dotenv
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse
from uvicorn import run as app_run

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logger
from networksecurity.pipeline.start_pipeline import TrainingPipeline
from networksecurity.utils.main_utils.utils import load_object
from networksecurity.constants.training_pipeline import (
    DATA_INGESTION_COLLECTION_NAME,
    DATA_INGESTION_DATABASE_NAME,
)

# ---------------- Environment & MongoDB ----------------
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL")

if not MONGO_DB_URL:
    logger.warning("MONGO_DB_URL not set; skipping MongoDB connection.")
    client = None
    collection = None
else:
    try:
        ca = certifi.where()
        client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=ca)
        database = client[DATA_INGESTION_DATABASE_NAME]
        collection = database[DATA_INGESTION_COLLECTION_NAME]
        logger.info("Connected to MongoDB successfully.")
    except Exception as e:
        logger.warning(f"MongoDB connection failed: {e}. Continuing without DB.")
        client = None
        collection = None

# ---------------- Helpers ----------------
def get_model_path() -> str:
    path = os.path.join("final_model", "model.pkl")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model file not found at {path}. Run /train first.")
    return path

def get_preprocessor_path() -> str:
    latest = os.path.join("Artifacts", "latest", "preprocessor.pkl")
    if os.path.exists(latest):
        return latest

    artifacts_root = "Artifacts"
    if not os.path.isdir(artifacts_root):
        raise FileNotFoundError("Artifacts folder not found. Run /train first.")

    candidates = [
        os.path.join(artifacts_root, d)
        for d in os.listdir(artifacts_root)
        if os.path.isdir(os.path.join(artifacts_root, d)) and d not in {"latest"}
    ]
    if not candidates:
        raise FileNotFoundError("No artifact runs found. Run /train first.")

    newest = max(candidates, key=os.path.getmtime)
    candidate_pp = os.path.join(
        newest, "data_transformation", "preprocessor", "preprocessor.pkl"
    )
    if os.path.exists(candidate_pp):
        return candidate_pp

    raise FileNotFoundError(
        f"Preprocessor not found. Looked for {latest} and {candidate_pp}. Run /train first."
    )

# ---------------- FastAPI app ----------------
app = FastAPI(title="Network Security ML API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", tags=["Root"])
async def root():
    return RedirectResponse(url="/docs")

@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok"}

@app.get("/train", tags=["Training"])
async def train_pipeline():
    try:
        tp = TrainingPipeline()
        tp.run_pipeline()
        return {"message": "Training pipeline executed successfully."}
    except Exception as e:
        logger.exception("Training pipeline failed.")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/predict", tags=["Prediction"])
async def predict(file: UploadFile = File(...)):
    try:
        df = pd.read_csv(file.file)

        # Drop CLASS_LABEL if present
        if "CLASS_LABEL" in df.columns:
            logger.info("Dropping CLASS_LABEL column from uploaded data")
            df = df.drop(columns=["CLASS_LABEL"])

        logger.info(f"Uploaded CSV shape: {df.shape}")
        logger.info(f"Uploaded CSV columns: {list(df.columns)}")

        # Load model + preprocessor
        model = load_object(get_model_path())
        preprocessor = load_object(get_preprocessor_path())
        logger.info("Model & Preprocessor loaded successfully")

        # Transform and predict
        transformed = preprocessor.transform(df)
        preds = model.predict(transformed)

        # Return only id + prediction (lightweight)
        response_df = pd.DataFrame()
        if "id" in df.columns:
            response_df["id"] = df["id"]
        response_df["prediction"] = preds

        return response_df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Prediction failed: {str(e)}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

if __name__ == "__main__":
    app_run(app, host="127.0.0.1", port=8888)
