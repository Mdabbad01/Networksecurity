from networksecurity.entity.config_entity import TrainingPipelineConfig, DataIngestionconfig
from networksecurity.components.data_ingestion import DataIngestion

pipeline_config = TrainingPipelineConfig()
data_ingestion_config = DataIngestionconfig(pipeline_config)
data_ingestion = DataIngestion(data_ingestion_config)

artifact = data_ingestion.initiate_data_ingestion()
print("Train CSV Path:", artifact.trained_file_path)
print("Test CSV Path:", artifact.test_file_path)
