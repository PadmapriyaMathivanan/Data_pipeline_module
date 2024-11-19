import os
import pandas as pd
import shutil
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
        logging_config.logger.info("Database connection established.")
        return engine
    except Exception as e:
        exceptions.handle_database_connection_error(e)
        raise

def copy_file_to_folder(file_path, destination_folder):
    """Copy file to the destination folder, keeping it also in the source location."""
    try:
        os.makedirs(destination_folder, exist_ok=True)
        shutil.copy(file_path, os.path.join(destination_folder, os.path.basename(file_path)))
        logging_config.logger.info(f"Copied file '{file_path}' to folder '{destination_folder}'")
    except Exception as e:
        exceptions.handle_file_copy_error(file_path, destination_folder, e)
        raise

def load_file_to_database(file_path, table_name, database_config, delimiter):
    """Append only new rows to the table in the database."""
    try:
        engine = create_database_connection(database_config)
        inspector = inspect(engine)
        data = pd.read_csv(file_path, delimiter=delimiter)
        
        if table_name in inspector.get_table_names():
            existing_data = pd.read_sql_table(table_name, engine)
            new_rows = data[~data.apply(tuple, axis=1).isin(existing_data.apply(tuple, axis=1))]
            if not new_rows.empty:
                new_rows.to_sql(table_name, engine, if_exists='append', index=False)
                logging_config.logger.info(f"Added {len(new_rows)} new rows to table '{table_name}'")
            else:
                logging_config.logger.info(f"No new rows to add for table '{table_name}'")
        else:
            data.to_sql(table_name, engine, if_exists='replace', index=False)
            logging_config.logger.info(f"Data loaded into new table '{table_name}'")
    except Exception as e:
        exceptions.handle_database_insertion_error(table_name, e)
        raise

def copy_database_to_database(source_db_config, destination_db_config, table_name):
    """Copy data from a source database to a destination database."""
    try:
        source_engine = create_database_connection(source_db_config)
        dest_engine = create_database_connection(destination_db_config)
        
        data = pd.read_sql_table(table_name, source_engine)
        
        # Insert data into the destination database, either appending or creating the table
        data.to_sql(table_name, dest_engine, if_exists='replace', index=False)
        
        logging_config.logger.info(f"Data copied from source database to destination database for table '{table_name}'")
    except Exception as e:
        exceptions.handle_database_copy_error(source_db_config, destination_db_config, table_name, e)
        raise
