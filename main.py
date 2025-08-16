from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataIngestionconfig, TrainingPipelineConfig, DataValidationConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact

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
        logging.info("Data ingestion completed")

        # Step 4: Data Validation config
        data_validation_config = DataValidationConfig(trainingpipelineconfig)

        # Step 5: Data Validation process
        data_validation = DataValidation(
            data_ingestion_artifact=dataingestionartifact,
            data_validation_config=data_validation_config
        )

        logging.info("Initiating the data validation process...")
        datavalidationartifact: DataValidationArtifact = data_validation.initiate_data_validation()
        logging.info("Data validation completed")

        # Step 6: Show results
        logging.info(f"Data ingestion and validation completed successfully.")
        logging.info(f"Train CSV Path: {dataingestionartifact.trained_file_path}")
        logging.info(f"Test CSV Path: {dataingestionartifact.test_file_path}")
        logging.info(f"Feature Store Path: {dataingestionconfig.feature_store_file_path}")

        logging.info(f"Validation Status: {datavalidationartifact.validation_status}")
        logging.info(f"Validation Train Path: {datavalidationartifact.valid_train_file_path}")
        logging.info(f"Validation Test Path: {datavalidationartifact.valid_test_file_path}")
        logging.info(f"Validation Message: {datavalidationartifact.message}")

        print("\n=== Data Ingestion Artifact ===")
        print(f"Train CSV Path: {dataingestionartifact.trained_file_path}")
        print(f"Test CSV Path: {dataingestionartifact.test_file_path}")
        print(f"Feature Store Path: {dataingestionconfig.feature_store_file_path}")

        print("\n=== Data Validation Artifact ===")
        print(f"Validation Status: {datavalidationartifact.validation_status}")
        print(f"Validation Train Path: {datavalidationartifact.valid_train_file_path}")
        print(f"Validation Test Path: {datavalidationartifact.valid_test_file_path}")
        print(f"Validation Message: {datavalidationartifact.message}")

    except Exception as e:
        raise NetworkSecurityException(e, sys)
