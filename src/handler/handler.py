from os import getenv
from datetime import datetime, timezone
import uuid
from helper.s3 import S3
from helper.postgres import PostgresSQL
from helper.logger import logger
from helper.helper import read_yaml, df_columns_normalization
from pandas import json_normalize
import traceback
import sys


def main(workflow_id):
    logger.info("Starting handler step.")
    try:
        uuid.UUID(str(workflow_id))
    except ValueError:
        logger.error(
            f"workflow_id are always UUIDs and the workflow_id '{workflow_id}' is not."
        )
        sys.exit(1)

    metadata_instance = PostgresSQL(
        dbname=getenv("DATA_WAREHOUSE_MONITOR_DB"),
        user=getenv("DATA_WAREHOUSE_USER"),
        password=getenv("DATA_WAREHOUSE_PASSWORD"),
        host=getenv("DATA_WAREHOUSE_HOST"),
        port=getenv("DATA_WAREHOUSE_PORT"),
    )

    s3_instance = S3(
        access_key=getenv("S3_DATA_ROOT_USER"),
        secret_access_key=getenv("S3_DATA_ROOT_PASSWORD"),
        host=getenv("S3_DATA_HOST"),
    )
    s3_bucket = getenv("S3_DATA_BUCKET")
    s3_instance.bucket_exists(s3_bucket)

    pg_instance = PostgresSQL(
        dbname=getenv("DATA_WAREHOUSE_DATA_DB"),
        user=getenv("DATA_WAREHOUSE_USER"),
        password=getenv("DATA_WAREHOUSE_PASSWORD"),
        host=getenv("DATA_WAREHOUSE_HOST"),
        port=getenv("DATA_WAREHOUSE_PORT"),
    )

    execution_metadata = {}
    execution_metadata["workflow_id"] = workflow_id
    execution_metadata["code_execution_id"] = str(uuid.uuid4())
    execution_metadata["code_execution_date"] = datetime.now(timezone.utc)
    execution_metadata["file_fetch_path"] = None
    try:
        schema_entities = read_yaml("./helper/schema_entities.yaml")
        entities = list(schema_entities.keys())
        for entity in entities:
            if not pg_instance.table_exists(schema_entities[entity]["table_name"]):
                raise Exception(
                    f"Required table '{schema_entities[entity]['table_name']}' does not exist. Please create it first."
                )

        s3_file_path = metadata_instance.get_ingestor_output_file_path(
            workflow_id=workflow_id
        )
        execution_metadata["file_fetch_path"] = s3_file_path

        if s3_file_path is None:
            logger.error(f"No valid .JSON file found for workflow {workflow_id}.")
        else:
            json_content = s3_instance.fetch_file_from_bucket(s3_file_path)
            entities_data = {entity: [] for entity in entities}
            for record in json_content:
                entities_data[record["on"]].append(record)

            for entity in entities:
                try:
                    table_name = schema_entities[entity]["table_name"]
                    execution_metadata[entity] = {"destination_table": table_name}
                    logger.info(f"Entity {entity} -- Table {table_name}")

                    df = json_normalize(entities_data[entity])
                    df_normalized = df_columns_normalization(
                        dataframe=df, column_schema=schema_entities[entity]["schema"]
                    )

                    pg_instance.insert_dataframe(
                        dataframe=df_normalized, table_name=table_name
                    )

                    execution_metadata[entity]["records_inserted"] = len(df_normalized)

                except Exception as e:
                    execution_metadata[entity]["traceback"] = traceback.format_exc()
                    logger.error(
                        f"Error processing/loading data to table {table_name}: {e}"
                    )

                finally:
                    metadata_instance.insert_metadata(
                        code_step="handler", metadata=execution_metadata, entity=entity
                    )

    except Exception as e:
        execution_metadata["traceback"] = traceback.format_exc()
        metadata_instance.insert_metadata(
            code_step="handler", metadata=execution_metadata
        )
        raise e

    finally:
        logger.info("handler step finished.")
        s3_instance.close()
        metadata_instance.close()
        pg_instance.close()
