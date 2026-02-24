# Network Security Phishing Detection System

## Overview

This project implements a machine learning-based phishing detection system using a structured pipeline architecture.

The system validates input data using a schema definition, performs model training, tracks experiments, and provides prediction capability through an application interface.

## System Components

- Data validation using schema  
- Data ingestion pipeline  
- Model training workflow  
- MLflow experiment tracking  
- MongoDB testing integration  
- Logging and monitoring  

## Dataset

The project utilizes a phishing dataset (phising_data.csv) for training and evaluation.

## Key Features

- Schema validation using YAML  
- Modular pipeline structure  
- MLflow integration  
- Docker support  
- CI workflow  
- Structured logging  

## Tech Stack

- Python  
- Scikit-learn  
- MLflow  
- MongoDB  
- Docker  
- YAML schema  

## How to Run

Install dependencies:

pip install -r requirements.txt

Run the training pipeline:

python main.py

Start the application:

python app.py

## Project Structure

├── src/
├── data_schema/
├── app.py
├── main.py
├── Dockerfile
├── schema.yaml
