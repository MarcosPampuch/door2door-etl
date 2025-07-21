from helper.logger import logger
import json
import yaml
import sys
import pandas as pd
import numpy as np
import hashlib
import uuid

def merge_jsons(json_list):
    """
    Merge a list of JSON objects or lists into a single flat list.
    
    Args:
        json_list (list): List of JSON objects or lists.
    
    Returns:
        list: Flattened list of JSON objects.
    """
    merged = []
    for item in json_list:
        if isinstance(item, list):
            merged.extend(item)
        else:
            merged.append(item)
    return merged

def save_json_locally(data, filename):
    """
    Save a JSON-serializable object to a local file.
    
    Args:
        data (object): JSON-serializable object to save.
        filename (str): Path to the output file.
    """
    
    with open(filename, 'w') as f:
        json.dump(data, f)

def read_yaml(file_path):
    """
    Read a YAML file and return its parsed content as a dictionary.
    
    Args:
        file_path (str): Path to the YAML file.
    
    Returns:
        dict: Parsed YAML content.
    """
    try:
        with open(file_path, 'r') as f:
            yaml_content = yaml.safe_load(f)
        
        return yaml_content
    except Exception as e:
        logger.error(f"Error reading YAML file ({file_path}): {e}")
        sys.exit(1)
    

def df_columns_normalization(dataframe, column_schema):
    """
    Normalize a DataFrame's columns according to a schema, apply type conversions, generate unique IDs, and drop duplicates.
    
    Args:
        dataframe (pd.DataFrame): Input DataFrame to normalize.
        column_schema (dict): Schema definition for columns.
    
    Returns:
        pd.DataFrame: Normalized DataFrame with unique event_generated_id and no duplicates.
    """
    source_to_pandas_type_mapping = {
        "uuid": pd.StringDtype(),
        "bigint": "Int64",
        "int": "Int64",
        "smallint": "Int64",
        "float": "float64",
        "varchar": pd.StringDtype(),
        "decimal": "float64",
        "timestamp": "datetime64[ms]",
        "date": "datetime64[s]",
        "char": pd.StringDtype(),
        "bit": "bool",
        "string": pd.StringDtype()
    }

    formated_df = pd.DataFrame()

    column_names_map = {original_col_name: column_specs['column_name'] for original_col_name, column_specs in column_schema.items()}
    column_types_map = {original_col_name: column_specs['type'] for original_col_name, column_specs in column_schema.items()}
    unique_identifier_columns = [column_specs['column_name'] for original_col_name, column_specs in column_schema.items() if column_specs.get('unique_identifier')]


    for original_column_name, column_type in column_types_map.items():
        try:
            pandas_type = source_to_pandas_type_mapping.get(column_type.lower())

            if pandas_type:
                if (column_type == "timestamp") or (column_type == "date"):
                    formated_df[original_column_name] = pd.to_datetime(dataframe[original_column_name],errors="coerce").dt.tz_localize(None)
                    # Convert NaT values to None for PostgreSQL compatibility
                    formated_df[original_column_name] = formated_df[original_column_name].replace({pd.NaT: None})

                else:
                    formated_df[original_column_name] = dataframe[original_column_name].astype(pandas_type)
                    # Convert NaN values to None for PostgreSQL compatibility
                    formated_df[original_column_name] = formated_df[original_column_name].replace({np.nan: None})

                if isinstance(pandas_type, pd.StringDtype):

                    formated_df[original_column_name] = formated_df[original_column_name].str.strip()

            else:
                raise Exception(
                    f'No dataframe type equivalent to "{column_type}" in "{original_column_name}".'
                )
            
        except Exception as err:
            raise err

    formated_df.rename(
            columns=column_names_map, inplace=True
        )
    
    formated_df['event_generated_id'] = formated_df.apply(_generate_unique_id, args=(unique_identifier_columns,) ,axis=1)

    
    formated_df = formated_df.drop_duplicates(subset=['event_generated_id'])

    
    return formated_df


def _generate_unique_id(row, unique_id_columns):
    """
    Generate a reproducible UUID for a row based on specified unique identifier columns.
    
    Args:
        row (pd.Series): DataFrame row.
        unique_id_columns (list): List of column names to use for ID generation.
    
    Returns:
        str: Generated UUID string.
    """
    combined = ''
    for column in unique_id_columns:
        
        column_string = str(row[column])
        
        combined += column_string
    
    hash_value = hashlib.sha256(combined.encode('utf-8')).hexdigest()
    
    return str(uuid.UUID(hash_value[:32]))  

    

def check_inputs_consistency(step, workflow=None):
    """
    Validate the consistency of input arguments for workflow execution.
    
    Args:
        step (str): The workflow step ('ingestor' or 'handler').
        workflow (str, optional): Workflow ID, required for handler step.
    
    Exits:
        If arguments are inconsistent or missing.
    """
    if workflow and step != 'handler':
        logger.error("workflow can only be declared when step mode is 'handler'.")
        sys.exit(1)
    
    if step == 'handler' and not workflow:
        logger.error("A workflow_id must be declared when step mode is 'handler'.")
        sys.exit(1)