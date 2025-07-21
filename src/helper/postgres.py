import psycopg2
from helper.logger import logger
import pandas as pd


class PostgresSQL:
    """
    PostgreSQL client wrapper for connecting, querying, and upserting transactions.
    """

    def __init__(self, dbname: str, user: str, password: str, host: str, port: str) -> None:
        """
        Initialize the PostgresSQL client and connect to the database.

        Args:
            dbname (str): Database name.
            user (str): Username.
            password (str): Password.
            host (str): Host address.
            port (str): Port number.
        """
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
        )

        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def insert_metadata(self, code_step, metadata, entity=None) -> None:
        """
        Insert execution metadata into the appropriate monitoring table (ingestor_executions or handler_executions).

        Args:
            code_step (str): 'ingestor' or 'handler'.
            metadata (dict): Metadata dictionary to insert.
            entity (str, optional): Entity name for handler step.
        """
        
        if code_step == 'ingestor':

            upsert_query = f"""
            INSERT INTO {code_step}_executions (workflow_id, code_execution_id, code_execution_date, fetched_hour, number_of_files_fetched, file_destination_path, traceback)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """

            self.cursor.execute(upsert_query, (
                metadata["workflow_id"],
                metadata["code_execution_id"],
                metadata["code_execution_date"],
                metadata.get("fetched_hour"),
                metadata.get("number_of_files_fetched"),
                metadata.get("file_destination_path"),
                metadata.get("traceback")
            ))

        elif code_step == 'handler':
            upsert_query = f"""
            INSERT INTO {code_step}_executions (workflow_id, code_execution_id, code_execution_date, file_fetch_path, destination_table, records_inserted, traceback)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """

            self.cursor.execute(upsert_query, (
                metadata["workflow_id"],
                metadata["code_execution_id"],
                metadata["code_execution_date"],
                metadata.get("file_fetch_path"),
                metadata.get(entity).get("destination_table") if entity else None,
                metadata.get(entity).get("records_inserted") if entity else None,
                metadata.get(entity).get("traceback") if entity else metadata.get("traceback")
            ))

    def get_last_successfull_fetch_date(self, code_step: str):
        """
        Return the latest successfully fetched hour from the specified executions table.

        Args:
            code_step (str): The code step ('ingestor' or 'handler').
        
        Returns:
            datetime or None: The maximum fetched_hour value or None if no rows match.
        """


        query = f"""
                    SELECT MAX(fetched_hour) 
                    FROM {code_step}_executions
                    WHERE traceback IS NULL;
                """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return result[0] if result else None

    def get_ingestor_output_file_path(self, workflow_id: str):
        """
        Return the file path of the ingestor output for a given workflow ID if the execution was successful.

        Args:
            workflow_id (str): Workflow ID to look up.
        
        Returns:
            str or None: File path if found, else None.
        """
        query = f"""
            SELECT file_destination_path
            FROM ingestor_executions
            WHERE workflow_id = '{workflow_id}'
            AND traceback IS NULL
            AND number_of_files_fetched > 0;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result is None:
            return None
        return result[0]

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name (str): Name of the table to check.
        
        Returns:
            bool: True if table exists, False otherwise.
        """
       
        check_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """
        self.cursor.execute(check_query)
        table_exists = self.cursor.fetchone()[0]
        
        if not table_exists:
            return False
        else:
            return True
        

    def insert_dataframe(self, dataframe: pd.DataFrame, table_name: str) -> None:
        """
        Insert a DataFrame into a PostgreSQL table, performing an upsert on event_generated_id.
        
        Args:
            dataframe (pd.DataFrame): DataFrame to insert.
            table_name (str): Name of the table to insert data into.
        """
        try:
            
           
            columns = list(dataframe.columns)
            
            placeholders = ', '.join(['%s'] * len(columns))
            update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])
            upsert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT (event_generated_id) DO UPDATE SET {update_set}
            """
            
            
            data_to_insert = [tuple(row) for row in dataframe.values]
            
            
            self.cursor.executemany(upsert_query, data_to_insert)
            
            logger.info(f"Successfully inserted {len(dataframe)} rows into table {table_name}")
            
        except Exception as e:
            logger.error(f"Error inserting DataFrame into table {table_name}: {e}")
            raise

    def close(self) -> None:
        """
        Close the database cursor and connection.
        """
        self.cursor.close()
        self.conn.close()