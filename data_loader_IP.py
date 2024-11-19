import os
import json
import logging
import hashlib
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Set up logging
logging.basicConfig(filename='data_loader.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_config():
    """Load the configuration from config.json."""
    with open('config.json', 'r') as file:
        config = json.load(file)
    return config

def create_database_connection(config):
    """Create a SQLAlchemy engine using the configuration."""
    try:
        connection_string = (
            f"postgresql+psycopg2://{config['database']['user']}:{config['database']['password']}@"
            f"{config['database']['host']}:{config['database']['port']}/{config['database']['database_name']}"
        )
        engine = create_engine(connection_string, pool_pre_ping=True)
        return engine
    except Exception as e:
        logging.error(f"Error creating database connection: {e}")
        raise

def send_email_notification(message):
    """Send an email notification if enabled in config."""
    try:
        if not config.get("notify_on_error"):
            return
        
        sender_email = "padmaparthi04@gmail.com"
        receiver_email = "priya13.madivanane@gmail.com"
        subject = "Data Loader Notification"
        email_body = message

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(email_body, 'plain'))

        with smtplib.SMTP('smtp.example.com', 587) as server:
            server.starttls()
            server.login(sender_email, "your_email_password")
            server.send_message(msg)
        
        logging.info("Notification email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send notification email: {e}")

def hash_file(file_path):
    """Generate a hash for the file to track changes."""
    hasher = hashlib.md5()
    with open(file_path, 'rb') as file:
        buf = file.read()
        hasher.update(buf)
    return hasher.hexdigest()

def is_file_processed(engine, file_name, file_hash):
    """Check if the file has already been processed based on its hash."""
    try:
        inspector = inspect(engine)
        if 'file_tracking' not in inspector.get_table_names():
            return False

        result = engine.execute(
            "SELECT 1 FROM file_tracking WHERE file_name = %s AND file_hash = %s",
            (file_name, file_hash)
        )
        return result.fetchone() is not None
    except SQLAlchemyError as e:
        logging.error(f"Error checking file processing status: {e}")
        return False

def track_file(engine, file_name, file_hash):
    """Track the processed file by storing its name and hash in the database."""
    try:
        engine.execute(
            "INSERT INTO file_tracking (file_name, file_hash) VALUES (%s, %s)",
            (file_name, file_hash)
        )
    except SQLAlchemyError as e:
        logging.error(f"Error tracking file: {e}")

def validate_schema(data, expected_columns):
    """Validate the schema of the data."""
    return set(expected_columns).issubset(data.columns)

def load_data_to_database(config):
    """Load data from files in the specified folder to the database."""
    try:
        engine = create_database_connection(config)
        file_path = config['file_path']
        file_types = config['file_type']
        load_options = config['load_options']

        for file_name in os.listdir(file_path):
            if not any(file_name.endswith(ft) for ft in file_types):
                continue

            full_path = os.path.join(file_path, file_name)
            file_hash = hash_file(full_path)

            if is_file_processed(engine, file_name, file_hash):
                logging.info(f"File {file_name} has already been processed. Skipping.")
                continue

            try:
                # Load data based on file type
                if file_name.endswith('.csv'):
                    data = pd.read_csv(full_path)
                elif file_name.endswith('.txt'):
                    data = pd.read_csv(full_path, delimiter='\t')
                else:
                    logging.warning(f"Unsupported file type for {file_name}. Skipping.")
                    continue

                # Validate schema if required
                if load_options.get("validate_schema"):
                    expected_columns = ["your", "expected", "columns"]
                    if not validate_schema(data, expected_columns):
                        logging.warning(f"Schema validation failed for {file_name}. Skipping.")
                        continue

                # Deduplicate data if required
                if load_options.get("deduplicate"):
                    data = data.drop_duplicates()

                # Load data into the database
                table_name = os.path.splitext(file_name)[0]
                data.to_sql(table_name, engine, if_exists=load_options.get("if_exists", "append"), index=False)
                logging.info(f"Data loaded successfully into table {table_name}.")

                # Track the processed file
                track_file(engine, file_name, file_hash)

            except Exception as e:
                logging.error(f"Error processing file {file_name}: {e}")
                if load_options.get("notify_on_error"):
                    send_email_notification(f"Error processing file {file_name}: {e}")

    except Exception as e:
        logging.error(f"Error loading data to database: {e}")
        if config['load_options'].get("notify_on_error"):
            send_email_notification(f"Critical error in data loader: {e}")

if __name__ == "__main__":
    config = load_config()
    load_data_to_database(config)
