from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionconfig, TrainingPipelineConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact

import sys

if __name__ == "__main__":
    try:
        # Step 1: Pipeline config
        trainingpipelineconfig = TrainingPipelineConfig()

        # Step 2: Data ingestion config
        dataingestionconfig = DataIngestionconfig(trainingpipelineconfig)

        # Step 3: Data ingestion process
        data_ingestion = DataIngestion(dataingestionconfig)

        logging.info("Initiating the data ingestion process...")
        dataingestionartifact: DataIngestionArtifact = data_ingestion.initiate_data_ingestion()

        # Step 4: Show results
        logging.info(f"Data ingestion completed successfully.")
        logging.info(f"Train CSV Path: {dataingestionartifact.trained_file_path}")
        logging.info(f"Test CSV Path: {dataingestionartifact.test_file_path}")
        logging.info(f"Feature Store Path: {dataingestionconfig.feature_store_file_path}")

        print("\n=== Data Ingestion Artifact ===")
        print(f"Train CSV Path: {dataingestionartifact.trained_file_path}")
        print(f"Test CSV Path: {dataingestionartifact.test_file_path}")
        print(f"Feature Store Path: {dataingestionconfig.feature_store_file_path}")

    except Exception as e:
        raise NetworkSecurityException(e, sys)
