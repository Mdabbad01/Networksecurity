# networksecurity/pipeline/start_pipeline.py

import sys

from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.components.model_trainer import ModelTrainer
from networksecurity.components.model_evaluation import ModelEvaluation

from networksecurity.entity.config_entity import (
    TrainingPipelineConfig,
    DataIngestionconfig,
    DataValidationConfig,
    DataTransformationConfig,
    ModelTrainerConfig,
    ModelEvaluationConfig,
)

from networksecurity.entity.artifact_entity import (
    DataIngestionArtifact,
    DataValidationArtifact,
    DataTransformationArtifact,
    ModelTrainerArtifact,
    ModelEvaluationArtifact,
)

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logger


class TrainingPipeline:
    def __init__(self):
        self.training_pipeline_config = TrainingPipelineConfig()

    def run_pipeline(self):
        try:
            logger.info("====== Starting Training Pipeline ======")

            # 1) Data Ingestion
            logger.info(">>> Initiating Data Ingestion...")
            data_ingestion_config = DataIngestionconfig(self.training_pipeline_config)
            data_ingestion = DataIngestion(data_ingestion_config)
            data_ingestion_artifact: DataIngestionArtifact = data_ingestion.initiate_data_ingestion()
            logger.info(">>> Data Ingestion Completed.")

            # 2) Data Validation
            logger.info(">>> Initiating Data Validation...")
            data_validation_config = DataValidationConfig(self.training_pipeline_config)
            data_validation = DataValidation(
                data_ingestion_artifact=data_ingestion_artifact,
                data_validation_config=data_validation_config,
            )
            data_validation_artifact: DataValidationArtifact = data_validation.initiate_data_validation()
            logger.info(">>> Data Validation Completed.")

            # 3) Data Transformation
            logger.info(">>> Initiating Data Transformation...")
            data_transformation_config = DataTransformationConfig(self.training_pipeline_config)
            data_transformation = DataTransformation(
                data_validation_artifact=data_validation_artifact,
                data_transformation_config=data_transformation_config,
            )
            data_transformation_artifact: DataTransformationArtifact = (
                data_transformation.initiate_data_transformation()
            )
            logger.info(">>> Data Transformation Completed.")

            # 4) Model Training
            logger.info(">>> Initiating Model Training...")
            model_trainer_config = ModelTrainerConfig(self.training_pipeline_config)
            model_trainer = ModelTrainer(
                data_transformation_artifact=data_transformation_artifact,
                model_trainer_config=model_trainer_config,
            )
            model_trainer_artifact: ModelTrainerArtifact = model_trainer.initiate_model_trainer()
            logger.info(">>> Model Training Completed.")

            # 5) Model Evaluation
            logger.info(">>> Initiating Model Evaluation...")
            model_evaluation_config = ModelEvaluationConfig(self.training_pipeline_config)
            model_evaluation = ModelEvaluation(
                model_evaluation_config=model_evaluation_config,
                model_trainer_artifact=model_trainer_artifact,
            )
            model_evaluation_artifact: ModelEvaluationArtifact = model_evaluation.initiate_model_evaluation()
            logger.info(">>> Model Evaluation Completed.")

            logger.info("====== Pipeline Execution Completed Successfully ======")

            # Optional: brief console summary
            print("\n=== SUMMARY ===")
            print(f"Train CSV          : {data_ingestion_artifact.trained_file_path}")
            print(f"Test CSV           : {data_ingestion_artifact.test_file_path}")
            print(f"Transformed Train  : {data_transformation_artifact.transformed_train_file_path}")
            print(f"Transformed Test   : {data_transformation_artifact.transformed_test_file_path}")
            print(f"Preprocessor       : {data_transformation_artifact.transformed_object_file_path}")
            print(f"Model (final)      : {model_trainer_artifact.trained_model_file_path}")
            print(f"Accepted?          : {model_evaluation_artifact.is_model_accepted}")

        except Exception as e:
            # Wrap any error in your custom exception (also gets logged by your logger)
            raise NetworkSecurityException(e, sys)


if __name__ == "__main__":
    # Run the pipeline when invoked directly (module or file)
    pipeline = TrainingPipeline()
    pipeline.run_pipeline()
