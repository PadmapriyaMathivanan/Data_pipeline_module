import pandas as pd
from sqlalchemy import create_engine, inspect
import logging_config
import exceptions

def create_database_connection(database_config):
    """Create a SQLAlchemy engine using the database configuration."""
    try:
        connection_string = (
            f"postgresql+psycopg2://{database_config['user']}:{database_config['password']}@"
            f"{database_config['host']}:{database_config['port']}/{database_config['database_name']}"
        )
        engine = create_engine(connection_string)
        logging_config.logger.info("Database connection established")
        return engine
    except Exception as e:
        exceptions.handle_database_connection_error(e)
        raise


def perform_feature_engineering(data, feature_config):
    """Perform feature engineering based on configuration."""
    try:
        if feature_config.get("remove_duplicates", False):
            data = data.drop_duplicates()
            logging_config.logger.info("Duplicates removed.")
        
        if feature_config.get("normalise_columns", False):
            numeric_columns = data.select_dtypes(include="number").columns
            
            # Explicitly assign using .loc to avoid SettingWithCopyWarning
            data.loc[:, numeric_columns] = (data[numeric_columns] - data[numeric_columns].min()) / (
                data[numeric_columns].max() - data[numeric_columns].min()
            )
            logging_config.logger.info("Numeric columns normalized.")
        
        return data
    except Exception as e:
        exceptions.handle_feature_engineering_error(e, "Feature Engineering")
        raise



def transfer_data_between_databases(source_db_config, dest_db_config, feature_config):
    """Copy data from source database to destination database with feature engineering."""
    try:
        # Create database connections
        source_engine = create_database_connection(source_db_config)
        dest_engine = create_database_connection(dest_db_config)

        # Get table names from source database
        inspector = inspect(source_engine)
        tables = inspector.get_table_names()

        for table_name in tables:
            logging_config.logger.info(f"Processing table '{table_name}'...")

            # Read data from source table
            data = pd.read_sql_table(table_name, source_engine)
            logging_config.logger.info(f"Read {len(data)} rows from table '{table_name}'.")

            # Perform feature engineering
            data = perform_feature_engineering(data, feature_config)

            # Write data to destination table
            data.to_sql(table_name, dest_engine, if_exists="replace", index=False)
            logging_config.logger.info(f"Transferred table '{table_name}' with {len(data)} rows to destination database.")
    except Exception as e:
        exceptions.handle_database_copy_error(source_db_config, dest_db_config, table_name, e)
        raise
