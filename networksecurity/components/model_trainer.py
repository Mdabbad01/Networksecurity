import os
import sys
import joblib
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, mean_squared_error, r2_score

import mlflow
import mlflow.sklearn

from networksecurity.entity.artifact_entity import (
    DataTransformationArtifact,
    ModelTrainerArtifact,
)
from networksecurity.entity.config_entity import ModelTrainerConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging


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
            # bump max_iter to reduce convergence warnings
            model = LogisticRegression(max_iter=1000, random_state=42, n_jobs=None)
            model.fit(X_train, y_train)
            return model
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_model_trainer(self) -> ModelTrainerArtifact:
        """
        Runs the full model training pipeline: load arrays, train, evaluate, save model,
        log everything to MLflow, and return an artifact object.
        """
        try:
            logging.info("Loading transformed train and test datasets...")

            # Load transformed train/test data
            train_arr = np.load(self.data_transformation_artifact.transformed_train_file_path)
            test_arr = np.load(self.data_transformation_artifact.transformed_test_file_path)

            # Split into X and y
            X_train, y_train = train_arr[:, :-1], train_arr[:, -1]
            X_test, y_test = test_arr[:, :-1], test_arr[:, -1]

            # Set up MLflow (local file storage by default)
            mlflow.set_tracking_uri("file:./mlruns")
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

                # RMSE (used for continuity with your artifact schema)
                train_rmse = float(np.sqrt(mean_squared_error(y_train, y_train_pred)))
                test_rmse = float(np.sqrt(mean_squared_error(y_test, y_test_pred)))

                # R^2 (not typical for classification; included because your artifact prints it)
                train_r2 = float(r2_score(y_train, y_train_pred))
                test_r2 = float(r2_score(y_test, y_test_pred))

                # Final score (use test accuracy)
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

                # ---- Log the model to MLflow
                mlflow.sklearn.log_model(model, artifact_path="model")

                # ---- Save the model locally (keeps your current contract)
                logging.info(f"Saving trained model at: {self.model_trainer_config.trained_model_file_path}")
                os.makedirs(os.path.dirname(self.model_trainer_config.trained_model_file_path), exist_ok=True)
                joblib.dump(model, self.model_trainer_config.trained_model_file_path)

            # Build and return artifact
            model_trainer_artifact = ModelTrainerArtifact(
                trained_model_file_path=self.model_trainer_config.trained_model_file_path,
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
