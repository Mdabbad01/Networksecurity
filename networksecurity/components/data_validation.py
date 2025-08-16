from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constants.training_pipeline import SCHEMA_FILE_PATH
from scipy.stats import ks_2samp
import pandas as pd
import os, sys
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):

        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        """
        Reads a CSV file into a pandas DataFrame.
        """
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        """
        Validate if dataframe has required number of columns.
        """
        try:
            number_of_columns = len(self._schema_config["columns"])
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"Dataframe has columns: {len(dataframe.columns)}")

            if len(dataframe.columns) == number_of_columns:
                return True
            return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def is_numerical_column_exist(self, dataframe: pd.DataFrame) -> bool:
        """
        Validate if all numerical columns mentioned in schema exist in dataframe.
        """
        try:
            numerical_columns = self._schema_config["numerical_columns"]
            dataframe_columns = dataframe.columns

            missing_numerical_columns = []
            for col in numerical_columns:
                if col not in dataframe_columns:
                    missing_numerical_columns.append(col)

            if len(missing_numerical_columns) > 0:
                logging.error(f"Missing numerical columns: {missing_numerical_columns}")
                return False

            logging.info("All numerical columns are present in the dataframe.")
            return True

        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def detect_dataset_drift(self, base_df, current_df, threshold=0.05) -> bool:
        """
        Detect dataset drift using KS test.
        Writes a YAML drift report to the path specified in config:
          self.data_validation_config.drift_report_file_path
        Returns:
          True if no drift detected (all columns p-value >= threshold),
          False if drift detected for >=1 column.
        """
        try:
            status = True
            report = {}
            # Use only columns present in both frames to avoid KeyError
            common_columns = [c for c in base_df.columns if c in current_df.columns]

            for column in common_columns:
                # For KS test we need numeric data; try to coerce to numeric and drop NA
                d1 = pd.to_numeric(base_df[column], errors="coerce").dropna()
                d2 = pd.to_numeric(current_df[column], errors="coerce").dropna()

                # If either series is empty after coercion, skip KS (treat as no-drift for that column)
                if d1.shape[0] == 0 or d2.shape[0] == 0:
                    report.update({column: {
                        "p_value": None,
                        "drift_status": False,
                        "note": "non-numeric or empty after coercion - skipped"
                    }})
                    continue

                is_same_dist = ks_2samp(d1, d2)
                p_value = float(is_same_dist.pvalue)

                if p_value >= threshold:
                    is_found = False  # no drift
                else:
                    is_found = True   # drift found
                    status = False

                report.update({column: {
                    "p_value": p_value,
                    "drift_status": is_found
                }})

            # write drift report
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path, exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path, content=report)

            return status
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        """
        Run the data validation checks:
        1. Number of columns
        2. Numerical columns existence
        3. Dataset drift check
        Saves validated files and prepares DataValidationArtifact with all required fields.
        """
        try:
            logging.info("Starting data validation...")

            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            # Read the data
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            error_message = ""

            # Validate number of columns (train)
            status = self.validate_number_of_columns(train_dataframe)
            if not status:
                error_message += "Train dataframe does not contain all required columns.\n"

            # Validate number of columns (test)
            status = self.validate_number_of_columns(test_dataframe)
            if not status:
                error_message += "Test dataframe does not contain all required columns.\n"

            # Validate numerical columns (train)
            status = self.is_numerical_column_exist(train_dataframe)
            if not status:
                error_message += "Train dataframe is missing some numerical columns.\n"

            # Validate numerical columns (test)
            status = self.is_numerical_column_exist(test_dataframe)
            if not status:
                error_message += "Test dataframe is missing some numerical columns.\n"

            # If any of the above failed, raise with collected message
            if error_message != "":
                raise Exception(error_message)

            # Dataset drift check
            status = self.detect_dataset_drift(base_df=train_dataframe, current_df=test_dataframe)
            if not status:
                logging.warning("Data drift detected between train and test datasets.")
            else:
                logging.info("No significant data drift detected between train and test datasets.")

            logging.info("Data validation completed successfully.")

            # Ensure directories exist for validated and invalid files and drift report
            valid_train_path = self.data_validation_config.valid_train_file_path
            valid_test_path = self.data_validation_config.valid_test_file_path
            drift_report_path = self.data_validation_config.drift_report_file_path

            # Derive invalid file paths next to validated files (if config doesn't provide them)
            invalid_train_path = getattr(self.data_validation_config, "invalid_train_file_path",
                                         os.path.join(os.path.dirname(valid_train_path), "invalid", "train.csv"))
            invalid_test_path = getattr(self.data_validation_config, "invalid_test_file_path",
                                        os.path.join(os.path.dirname(valid_test_path), "invalid", "test.csv"))

            os.makedirs(os.path.dirname(valid_train_path), exist_ok=True)
            os.makedirs(os.path.dirname(valid_test_path), exist_ok=True)
            os.makedirs(os.path.dirname(invalid_train_path), exist_ok=True)
            os.makedirs(os.path.dirname(invalid_test_path), exist_ok=True)
            os.makedirs(os.path.dirname(drift_report_path), exist_ok=True)

            # Save validated CSVs
            train_dataframe.to_csv(valid_train_path, index=False, header=True)
            test_dataframe.to_csv(valid_test_path, index=False, header=True)

            # Create empty invalid CSV placeholders (will be overwritten later if you implement invalid-row logic)
            pd.DataFrame().to_csv(invalid_train_path, index=False)
            pd.DataFrame().to_csv(invalid_test_path, index=False)

            # Prepare and return DataValidationArtifact (all required fields present)
            return DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=valid_train_path,
                valid_test_file_path=valid_test_path,
                invalid_train_file_path=invalid_train_path,
                invalid_test_file_path=invalid_test_path,
                drift_report_file=drift_report_path,
                message="Data validation completed successfully"
            )

        except Exception as e:
            raise NetworkSecurityException(e, sys)
