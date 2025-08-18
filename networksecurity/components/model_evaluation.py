import os
import sys
import joblib
from sklearn.metrics import accuracy_score
from networksecurity.entity.artifact_entity import ModelTrainerArtifact, ModelEvaluationArtifact
from networksecurity.entity.config_entity import ModelEvaluationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

class ModelEvaluation:
    def __init__(self, model_evaluation_config: ModelEvaluationConfig, model_trainer_artifact: ModelTrainerArtifact):
        try:
            self.model_evaluation_config = model_evaluation_config
            self.model_trainer_artifact = model_trainer_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_model_evaluation(self) -> ModelEvaluationArtifact:
        try:
            logging.info("Starting model evaluation...")

            trained_model_path = self.model_trainer_artifact.trained_model_file_path
            trained_model = joblib.load(trained_model_path)

            # If no previous model exists, accept new model
            if not os.path.exists(self.model_evaluation_config.best_model_path):
                logging.info("No existing model found. Accepting new model as best.")
                return ModelEvaluationArtifact(
                    is_model_accepted=True,
                    evaluated_model_path=trained_model_path,
                    best_model_path=trained_model_path,
                    message="New model accepted as the first model."
                )

            # Load previous best model
            best_model = joblib.load(self.model_evaluation_config.best_model_path)

            # Evaluate both on the same test set (from trainer artifact)
            # We'll just compare test accuracy
            new_model_score = self.model_trainer_artifact.test_accuracy
            best_model_score = self.model_trainer_artifact.model_accuracy  # from last run

            if new_model_score > best_model_score:
                logging.info("New model is better. Accepting new model.")
                return ModelEvaluationArtifact(
                    is_model_accepted=True,
                    evaluated_model_path=trained_model_path,
                    best_model_path=self.model_evaluation_config.best_model_path,
                    message="New model accepted and will replace the best model."
                )
            else:
                logging.info("New model is worse. Keeping old model.")
                return ModelEvaluationArtifact(
                    is_model_accepted=False,
                    evaluated_model_path=trained_model_path,
                    best_model_path=self.model_evaluation_config.best_model_path,
                    message="New model rejected. Keeping existing best model."
                )

        except Exception as e:
            raise NetworkSecurityException(e, sys)
