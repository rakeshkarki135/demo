import logging
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger("db_connect")


class DbConnection:
    def __init__(self):
        self.engine = None
        self.Session = None

    def get_database_connection(self, username, password, db_host, db_port, db_name):
        """Establish a database connection and return the session factory"""
        try:
            connection_string = (
                f"mysql+pymysql://{username}:{password}@{db_host}:{db_port}/{db_name}?connect_timeout=600"
            )
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            self.Session = sessionmaker(bind=self.engine)
        except Exception as e:
            logger.error(f"Failed to establish database connection: {str(e)}", exc_info=True)
            raise

    def get_server_connection(self):
        """Initialize a database connection and return the DbConnection instance"""
        try:
            self.get_database_connection(
                config("DB_USERNAME"),
                config("DB_PASSWORD"),
                config("DB_HOST"),
                config("DB_PORT"),
                config("DB_DATABASE"),
            )
            return self  # Return the instance to allow session creation in insert_to_db.py
        except Exception as e:
            logger.error(f"Failed to get server connection: {str(e)}", exc_info=True)
            raise
