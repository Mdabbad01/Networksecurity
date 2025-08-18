from dataclasses import dataclass


@dataclass
class DataIngestionArtifact:
    trained_file_path:str
    test_file_path:str
    
@dataclass
class DataValidationArtifact:
    validation_status:bool
    valid_train_file_path:str
    valid_test_file_path:str
    invalid_train_file_path:str
    invalid_test_file_path:str
    drift_report_file:str
    message: str



@dataclass
class DataTransformationArtifact:
    transformed_train_file_path: str
    transformed_test_file_path: str
    transformed_object_file_path: str  
    message: str = "Data Transformation completed successfully"
    
    
from dataclasses import dataclass

@dataclass
class ModelTrainerArtifact:
    trained_model_file_path: str
    train_rmse: float
    test_rmse: float
    train_accuracy: float
    test_accuracy: float
    model_accuracy: float
    message: str
    train_r2: float    
    test_r2: float  
    
@dataclass
class ModelEvaluationArtifact:
    is_model_accepted: bool
    evaluated_model_path: str
    best_model_path: str
    message: str
