from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.components.model_trainer import ModelTrainer

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.components.model_evaluation import ModelEvaluation
from networksecurity.entity.config_entity import ModelEvaluationConfig
from networksecurity.entity.artifact_entity import ModelEvaluationArtifact


from networksecurity.entity.config_entity import (
    DataIngestionconfig,
    TrainingPipelineConfig,
    DataValidationConfig,
    DataTransformationConfig,
    ModelTrainerConfig,
)

from networksecurity.entity.artifact_entity import (
    DataIngestionArtifact,
    DataValidationArtifact,
    DataTransformationArtifact,
    ModelTrainerArtifact,
)

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
            data_validation_config=data_validation_config,
        )

        logging.info("Initiating the data validation process...")
        datavalidationartifact: DataValidationArtifact = data_validation.initiate_data_validation()
        logging.info("Data validation completed")

        # Step 6: Data Transformation config
        data_transformation_config = DataTransformationConfig(trainingpipelineconfig)

        # Step 7: Data Transformation process
        data_transformation = DataTransformation(
            data_validation_artifact=datavalidationartifact,
            data_transformation_config=data_transformation_config,
        )

        logging.info("Initiating the data transformation process...")
        datatransformationartifact: DataTransformationArtifact = (
            data_transformation.initiate_data_transformation()
        )
        logging.info("Data transformation completed")

        # Step 8: Model Trainer config
        model_trainer_config = ModelTrainerConfig(trainingpipelineconfig)

        # Step 9: Model Trainer process
        model_trainer = ModelTrainer(
            data_transformation_artifact=datatransformationartifact,
            model_trainer_config=model_trainer_config,
        )

        logging.info("Initiating the model training process...")
        modeltrainerartifact: ModelTrainerArtifact = model_trainer.initiate_model_trainer()
        logging.info("Model training completed")

        # Step 10: Show results
        logging.info("Pipeline execution completed successfully.")
        
        logging.info("Initiating the model evaluation process...")
        model_evaluation_config = ModelEvaluationConfig(trainingpipelineconfig)
        model_evaluation = ModelEvaluation(
          model_evaluation_config=model_evaluation_config,
         model_trainer_artifact=modeltrainerartifact
)
        modelevaluationartifact: ModelEvaluationArtifact = model_evaluation.initiate_model_evaluation()
        logging.info("Model evaluation completed")

        print("\n=== Data Ingestion Artifact ===")
        print(f"Train CSV Path: {dataingestionartifact.trained_file_path}")
        print(f"Test CSV Path: {dataingestionartifact.test_file_path}")
        print(f"Feature Store Path: {dataingestionconfig.feature_store_file_path}")

        print("\n=== Data Validation Artifact ===")
        print(f"Validation Status: {datavalidationartifact.validation_status}")
        print(f"Validation Train Path: {datavalidationartifact.valid_train_file_path}")
        print(f"Validation Test Path: {datavalidationartifact.valid_test_file_path}")
        print(f"Validation Message: {datavalidationartifact.message}")

        print("\n=== Data Transformation Artifact ===")
        print(f"Transformed Train Path: {datatransformationartifact.transformed_train_file_path}")
        print(f"Transformed Test Path: {datatransformationartifact.transformed_test_file_path}")
        print(f"Preprocessor Object Path: {datatransformationartifact.transformed_object_file_path}")

        print("\n=== Model Trainer Artifact ===")
        print(f"Trained Model Path: {modeltrainerartifact.trained_model_file_path}")
        print(f"Train RMSE: {modeltrainerartifact.train_rmse}")
        print(f"Test RMSE: {modeltrainerartifact.test_rmse}")
        print(f"Train Accuracy: {modeltrainerartifact.train_accuracy}")
        print(f"Test Accuracy: {modeltrainerartifact.test_accuracy}")
        print(f"Model Accuracy (selected): {modeltrainerartifact.model_accuracy}")
        print(f"Train R2 Score: {modeltrainerartifact.train_r2}")
        print(f"Test R2 Score: {modeltrainerartifact.test_r2}")
        
        print("\n=== Model Evaluation Artifact ===")
        print(f"Is Model Accepted: {modelevaluationartifact.is_model_accepted}")
        print(f"Evaluated Model Path: {modelevaluationartifact.evaluated_model_path}")
        print(f"Best Model Path: {modelevaluationartifact.best_model_path}")
        print(f"Message: {modelevaluationartifact.message}")
        
        

    except Exception as e:
        raise NetworkSecurityException(e, sys)
