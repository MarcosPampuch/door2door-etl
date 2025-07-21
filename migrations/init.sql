CREATE DATABASE monitor_db;


\connect monitor_db

CREATE TABLE ingestor_executions (
    workflow_id UUID,
    code_execution_id UUID,
    code_execution_date TIMESTAMP,
    fetched_hour TIMESTAMP,
    number_of_files_fetched INTEGER,
    file_destination_path VARCHAR(255),
    traceback TEXT
);

CREATE TABLE handler_executions (
    workflow_id UUID,
    code_execution_id UUID,
    code_execution_date TIMESTAMP,
    file_fetch_path VARCHAR(255),
    destination_table VARCHAR(255),
    records_inserted INTEGER,
    traceback TEXT
);


\connect data_warehouse_db

CREATE TABLE vehicle_location (
	event_generated_id UUID PRIMARY KEY,
    vehicle_id UUID,
    event_timestamp TIMESTAMP NOT NULL,
    event_operation VARCHAR(255),
    organization_id VARCHAR(255),
    vehicle_latitude FLOAT,
    vehicle_longitude FLOAT,
    vehicle_location_timestamp TIMESTAMP,
    original_s3_file_path VARCHAR(255)
);


CREATE TABLE operating_periods (
	event_generated_id UUID PRIMARY KEY,
    operating_period_id VARCHAR(255),
    event_timestamp TIMESTAMP NOT NULL,
    event_operation VARCHAR(255),
    organization_id VARCHAR(255),
    operation_start TIMESTAMP,
    operation_finish TIMESTAMP,
    original_s3_file_path VARCHAR(255)
);

