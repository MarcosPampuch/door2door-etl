from os import getenv
import uuid
from datetime import datetime, timezone, timedelta
from helper.logger import logger
from helper.s3 import S3
from helper.postgres import PostgresSQL
from helper.helper import save_json_locally, merge_jsons
import traceback


def main(workflow_id):
    logger.info("Starting ingestor step.")
    execution_metadata = {}
    execution_id = str(uuid.uuid4())
    current_datetime = datetime.now(timezone.utc)

    s3_anon_instance = S3(anonymous=True)
    s3_bucket = getenv("S3_BUCKET")

    s3_data_instance = S3(
        access_key=getenv("S3_DATA_ROOT_USER"),
        secret_access_key=getenv("S3_DATA_ROOT_PASSWORD"),
        host=getenv("S3_DATA_HOST"),
    )
    s3_data_bucket = getenv("S3_DATA_BUCKET")
    s3_data_instance.bucket_exists(s3_data_bucket)

    s3_instance = PostgresSQL(
        dbname=getenv("DATA_WAREHOUSE_MONITOR_DB"),
        user=getenv("DATA_WAREHOUSE_USER"),
        password=getenv("DATA_WAREHOUSE_PASSWORD"),
        host=getenv("DATA_WAREHOUSE_HOST"),
        port=getenv("DATA_WAREHOUSE_PORT"),
    )

    try:
        output_filename = (
            f"{execution_id}_{current_datetime.strftime('%Y%m%dT%H%M%SZ')}.json"
        )
        logger.info(
            f"Execution started. UUID: {execution_id}, Timestamp: {current_datetime.isoformat()}"
        )

        last_fetch_date = s3_instance.get_last_successfull_fetch_date(
            code_step="ingestor"
        )
        if last_fetch_date is not None:
            code_fetch_date = last_fetch_date + timedelta(hours=1)
        else:
            code_fetch_date = datetime(2022, 11, 24, 10, 0, 0, tzinfo=timezone.utc)

        logger.info(f"Fetching data from hour: {code_fetch_date}.")

        jsons, number_of_files_fetched = s3_anon_instance.get_hour_files_from_bucket(
            s3_bucket, code_fetch_date
        )
        if not jsons:
            logger.warning("No JSON files found for this hour.")
        else:
            logger.info(f"Fetched {number_of_files_fetched} files from S3 bucket.")
            merged_json = merge_jsons(jsons)
            save_json_locally(merged_json, output_filename)
            s3_data_instance.upload_file_to_bucket(output_filename, s3_data_bucket)

    except Exception as e:
        execution_metadata["traceback"] = traceback.format_exc()
        raise e
    finally:
        logger.info("ingestor step finished.\n")
        s3_anon_instance.close()
        s3_data_instance.close()

        execution_metadata["workflow_id"] = workflow_id
        execution_metadata["code_execution_id"] = execution_id
        execution_metadata["code_execution_date"] = current_datetime
        execution_metadata["fetched_hour"] = code_fetch_date
        execution_metadata["number_of_files_fetched"] = number_of_files_fetched
        execution_metadata["file_destination_path"] = (
            f"s3://{s3_data_bucket}/{output_filename}"
        )

        s3_instance.insert_metadata(code_step="ingestor", metadata=execution_metadata)
        s3_instance.close()


if __name__ == "__main__":
    main()
