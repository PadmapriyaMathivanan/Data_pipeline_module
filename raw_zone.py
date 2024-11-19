import os
import pandas as pd
import shutil
import logging
import logging_config
from sqlalchemy import create_engine, inspect
import exceptions  # Import the exceptions module

def create_database_connection(database_config):
    """Create a SQLAlchemy engine using the database configuration."""
    try:
        connection_string = (
            f"postgresql+psycopg2://{database_config['user']}:{database_config['password']}@"
            f"{database_config['host']}:{database_config['port']}/{database_config['database_name']}"
        )
        engine = create_engine(connection_string)
        logging_config.logger.info("Database connection established")
        #logging.info("Database connection established.")
        return engine
    except Exception as e:
        exceptions.handle_database_connection_error(e)
        raise

def copy_file_to_folder(file_path, destination_folder):
    """Copy file from source folder to destination folder."""
    try:
        
        os.makedirs(destination_folder, exist_ok=True)
        shutil.copy(file_path, os.path.join(destination_folder, os.path.basename(file_path)))
        logging_config.logger.info(f"Copied file '{file_path}' to folder '{destination_folder}'")
    except Exception as e:
        exceptions.handle_file_copy_error(file_path, destination_folder, e)
        raise

def append_new_data_to_table(file_path, table_name, engine, delimiter):
    """Append only new data to an existing table in the database."""
    try:
        logging_config.logger.info(f"Processing file: {file_path}, Table: {table_name}")
        inspector = inspect(engine)
        new_data = pd.read_csv(file_path, delimiter=delimiter)
        
        if table_name in inspector.get_table_names():
            existing_data = pd.read_sql_table(table_name, engine)
            # Ensure that the columns in new_data and existing_data are identical for comparison
            common_columns = new_data.columns.intersection(existing_data.columns)

            # Merge to identify unique rows only in new_data
            unique_new_data = pd.merge(
                new_data[common_columns],
                existing_data[common_columns],
                how='left',
                indicator=True
            ).query("_merge == 'left_only'").drop(columns=['_merge'])

            # Check if there are new rows to insert
            if not unique_new_data.empty:
                unique_new_data.to_sql(table_name, engine, if_exists='append', index=False)
                logging_config.logger.info(f"Added {len(unique_new_data)} new rows to table '{table_name}'")
            else:
                logging_config.logger.info(f"No new rows to add for table '{table_name}'")
                
        else:
            new_data.to_sql(table_name, engine, if_exists='replace', index=False)
            logging_config.logger.info(f"Data loaded into new table '{table_name}'")
    except Exception as e:
        exceptions.handle_database_insertion_error(table_name, e)
        raise

def transfer_data_between_databases(source_engine, dest_engine, table_name):
    """Copy data from a table in the source database to a table in the destination database."""
    try:
        source_data = pd.read_sql_table(table_name, source_engine)
        source_data.to_sql(table_name, dest_engine, if_exists='replace', index=False)
        logging_config.logger.info(f"Transferred table '{table_name}' from source to destination database.")
    except Exception as e:
        exceptions.handle_database_insertion_error(table_name, e)
        raise

