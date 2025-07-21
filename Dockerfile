FROM python:3.11-slim

WORKDIR /opt/etl-door2door/
COPY ./src .

RUN pip install --no-cache-dir -r requirements.txt

ENV S3_BUCKET="de-tech-assessment-2022"
ENV S3_DATA_BUCKET="door2door-files"
ENV S3_DATA_ROOT_USER="admin"
ENV S3_DATA_ROOT_PASSWORD="password1234"
ENV S3_DATA_HOST="minio:9000"
ENV DATA_WAREHOUSE_HOST="postgres"
ENV DATA_WAREHOUSE_USER="admin"
ENV DATA_WAREHOUSE_PASSWORD="password1234"
ENV DATA_WAREHOUSE_PORT="5432"
ENV DATA_WAREHOUSE_MONITOR_DB='monitor_db'
ENV DATA_WAREHOUSE_DATA_DB='data_warehouse_db'

CMD ["python3", "executor.py", "--help"]
