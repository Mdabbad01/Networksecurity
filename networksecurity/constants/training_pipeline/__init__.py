import os
import sys
import numpy as np
import pandas as pd


"""
defining common constant variable for the training 

"""
TARGET_COLUMN  ="CLASS_LABEL"
PIPELINE_NAME: str="NetworkSecurity"
ARTIFACT_DIR: str = "Artifacts"
FILE_NAME: str = "phising_data.csv"

TRAIN_FILE_NAME = "train.csv"
TEST_FILE_NAME = "test.csv"

SCHEMA_FILE_PATH= os.path.join("data_schema" , "schema.yaml")


"""
Data ingestion related constants start with DATA_INGESTION_ prefix
"""


DATA_INGESTION_COLLECTION_NAME: str = "NetworkData"
DATA_INGESTION_DATABASE_NAME: str = "MD_abbad"
DATA_INGESTION_DIR_NAME: str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR: str = "feature_store"
DATA_INGESTION_INGESTED_DIR:str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO: float = 0.2


"""

data validation related related constant start with DATA_VALIDATION  VAR NAME

"""
DATA_VALIDATION_DIR_NAME: str = "data_validation"

DATA_VALIDATION_VALID_DIR:str = "validated"

DATA_VALIDATION_INVALID_DIR: str = "invalid"
DATA_VALIDATION_DRIFT_REPORT_DIR: str = "drift_report"
DATA_VALIDATION_DRIFT_REPORT_FILE_NAME : str = "report.yaml"



"""
data transformation constants
"""
DATA_TRANSFORMATION_DIR_NAME :str= "data_transformation"
DATA_TRANSFORMATION_TRANSFORMED_DATA_DIR :str= "transformed"
DATA_TRANSFORMATION_PREPROCESSOR_DIR: str = "preprocessor"
PREPROCESSING_OBJECT_FILE_NAME :str= "preprocessor.pkl"



DATA_TRANSFORMATION_IMPUTER_PARAMS: dict = {
    "missing_values":np.nan,
    "n_neighbors":3,
    "weights":"uniform"
}

# Model Trainer constants
MODEL_TRAINER_DIR_NAME = "model_trainer"
MODEL_TRAINER_TRAINED_MODEL_DIR = "trained_model"
MODEL_FILE_NAME = "model.pkl"

# Default hyperparameters for training (you can tweak later)
MODEL_TRAINER_PARAMS = {
    "random_state": 42,
    "n_estimators": 100
}

# Baseline accuracy (model must beat this, else retrain / reject)
MODEL_TRAINER_BASE_ACCURACY = 0.6

MLFLOW_TRACKING_URI = "http://127.0.0.1:5000"  # if you run mlflow server
MLFLOW_EXPERIMENT_NAME = "NetworkSecurityExperiment"












