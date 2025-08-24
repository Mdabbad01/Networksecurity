# Use slim Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy only necessary files
COPY requirements.txt /app/
COPY app.py /app/
COPY networksecurity/ /app/networksecurity/

# Install dependencies and clear pip cache to reduce image size
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Expose FastAPI port
EXPOSE 8888

# Set environment variables (MLflow)
ENV MLFLOW_TRACKING_URI=http://127.0.0.1:5000

# Command to run FastAPI
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8888"]
