import sys
import os
import pandas as pd
from networksecurity.utils.main_utils import load_object
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logger   # ✅ FIXED


class BatchPrediction:
    def __init__(self, model_path: str, preprocessor_path: str):
        try:
            self.model = load_object(model_path)
            self.preprocessor = load_object(preprocessor_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def predict(self, input_file_path: str, output_file_path: str = "prediction.csv"):
        try:
            logger.info(f"Loading input data from {input_file_path}")
            data = pd.read_csv(input_file_path)

            logger.info("Transforming input data...")
            transformed_data = self.preprocessor.transform(data)

            logger.info("Making predictions...")
            predictions = self.model.predict(transformed_data)

            data["prediction"] = predictions
            data.to_csv(output_file_path, index=False)

            logger.info(f"Predictions saved to {output_file_path}")
            return output_file_path
        except Exception as e:
            raise NetworkSecurityException(e, sys)
