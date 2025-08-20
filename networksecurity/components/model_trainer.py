import os
import sys
import joblib
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, mean_squared_error, r2_score

# DagsHub MLflow: avoid "logged models" endpoint
os.environ["MLFLOW_ENABLE_LOGGED_MODELS"] = "false"

import mlflow
import dagshub

from networksecurity.entity.artifact_entity import (
    DataTransformationArtifact,
    ModelTrainerArtifact,
)
from networksecurity.entity.config_entity import ModelTrainerConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.utils.main_utils import save_object  # now correctly exported


# Initialize DagsHub MLflow tracking (before starting runs)
dagshub.init(repo_owner="Mdabbad01", repo_name="Networksecurity", mlflow=True)


class ModelTrainer:
    def __init__(
        self,
        model_trainer_config: ModelTrainerConfig,
        data_transformation_artifact: DataTransformationArtifact,
    ):
        """
        Model Trainer component: trains a model using transformed data and logs to MLflow.
        """
        try:
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifact = data_transformation_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def train_model(self, X_train: np.ndarray, y_train: np.ndarray):
        """
        Train a simple classifier (Logistic Regression).
        """
        try:
            logging.info("Training Logistic Regression model...")
            model = LogisticRegression(max_iter=1000, random_state=42, n_jobs=None)
            model.fit(X_train, y_train)
            return model
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_model_trainer(self) -> ModelTrainerArtifact:
        """
        Load arrays, train, evaluate, save model locally and to final_model/,
        and log metrics & artifacts to MLflow (DagsHub).
        """
        try:
            logging.info("Loading transformed train and test datasets...")

            # Load transformed train/test data
            train_arr = np.load(self.data_transformation_artifact.transformed_train_file_path)
            test_arr = np.load(self.data_transformation_artifact.transformed_test_file_path)

            # Split into X and y
            X_train, y_train = train_arr[:, :-1], train_arr[:, -1]
            X_test, y_test = test_arr[:, :-1], test_arr[:, -1]

            # Experiment name
            mlflow.set_experiment("NetworkSecurityExperiment")

            with mlflow.start_run(run_name="LogReg_Train"):
                # ---- Train
                model = self.train_model(X_train, y_train)

                # ---- Evaluate
                logging.info("Evaluating model performance...")
                y_train_pred = model.predict(X_train)
                y_test_pred = model.predict(X_test)

                train_accuracy = float(accuracy_score(y_train, y_train_pred))
                test_accuracy = float(accuracy_score(y_test, y_test_pred))

                train_rmse = float(np.sqrt(mean_squared_error(y_train, y_train_pred)))
                test_rmse = float(np.sqrt(mean_squared_error(y_test, y_test_pred)))

                # (R2 is odd for classification, but kept to match your artifact schema)
                train_r2 = float(r2_score(y_train, y_train_pred))
                test_r2 = float(r2_score(y_test, y_test_pred))

                model_accuracy = float(test_accuracy)

                # ---- Log params & metrics to MLflow
                mlflow.log_param("model_type", "LogisticRegression")
                mlflow.log_param("max_iter", 1000)
                mlflow.log_param("random_state", 42)
                mlflow.log_metric("train_accuracy", train_accuracy)
                mlflow.log_metric("test_accuracy", test_accuracy)
                mlflow.log_metric("train_rmse", train_rmse)
                mlflow.log_metric("test_rmse", test_rmse)
                mlflow.log_metric("train_r2", train_r2)
                mlflow.log_metric("test_r2", test_r2)

                # ---- Save model to your configured artifacts path (optional, keeps your pattern)
                local_artifact_model_path = self.model_trainer_config.trained_model_file_path
                os.makedirs(os.path.dirname(local_artifact_model_path), exist_ok=True)
                joblib.dump(model, local_artifact_model_path)
                # Upload that file as an artifact (safe for DagsHub)
                mlflow.log_artifact(local_artifact_model_path, artifact_path="model_files")

                # ---- ALSO save to final_model/model.pkl (as your tutor expects)
                final_model_path = os.path.join("final_model", "model.pkl")
                save_object(final_model_path, model)
                # Upload the final model too
                mlflow.log_artifact(final_model_path, artifact_path="final_model")

            # Return artifact pointing at the final model path (so your prints match)
            model_trainer_artifact = ModelTrainerArtifact(
                trained_model_file_path=final_model_path,
                train_rmse=train_rmse,
                test_rmse=test_rmse,
                train_accuracy=train_accuracy,
                test_accuracy=test_accuracy,
                model_accuracy=model_accuracy,
                message="Model training completed successfully",
                train_r2=train_r2,
                test_r2=test_r2,
            )

            logging.info(f"Model Trainer Artifact: {model_trainer_artifact}")
            return model_trainer_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)
