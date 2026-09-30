FROM ghcr.io/mlflow/mlflow:latest

# The upstream image does not include the database/artifact-store drivers used
# by this repository's configured PostgreSQL + S3 backend.
RUN pip install --no-cache-dir mlflow==3.9.0 psycopg2-binary boto3
