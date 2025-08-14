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
 # for example


"""
Data ingestion related constants start with DATA_INGESTION_ prefix
"""


DATA_INGESTION_COLLECTION_NAME: str = "NetworkData"
DATA_INGESTION_DATABASE_NAME: str = "MD_abbad"
DATA_INGESTION_DIR_NAME: str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR: str = "feature_store"
DATA_INGESTION_INGESTED_DIR:str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO: float = 0.2







