from sqlalchemy import create_engine, inspect
import yaml

class DatabaseConnector:
    def __init__(self, creds_file):
        self.engine = self.init_db_engine(creds_file)

    @staticmethod
    def read_db_creds(creds_file):
        """
        Read database credentials from a YAML file.

        Args:
        - creds_file (str): Path to the YAML file containing credentials.

        Returns:
        - dict: Dictionary containing database credentials.
        """
        try:
            with open(creds_file, 'r') as file:
                creds = yaml.safe_load(file)
            return creds
        except FileNotFoundError:
            print("Credentials file not found.")
            return None
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
            return None

    @staticmethod
    def init_db_engine(creds_file):
        """
        Initialize and return an SQLAlchemy database engine.

        Args:
        - creds_file (str): Path to the YAML file containing credentials.

        Returns:
        - sqlalchemy.engine.base.Engine: SQLAlchemy database engine.
        """
        creds = DatabaseConnector.read_db_creds(creds_file)
        if creds:
            try:
                # Construct database URL
                db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
                
                # Create engine
                engine = create_engine(db_url)
                return engine
            except Exception as e:
                print(f"Error initializing database engine: {e}")
                return None
        else:
            print("Error initializing database engine: Credentials not found.")
            return None
        
    def list_db_tables(self):
        """
        List all tables in the database.

        Returns:
        - list: List of table names.
        """
        if self.engine:
            try:
                # Create an inspector for the engine
                inspector = inspect(self.engine)

                # Get the table names
                table_names = inspector.get_table_names()
                print("Tables in the database:")
                for table_name in table_names:
                    print(table_name)
                return table_names
            except Exception as e:
                print(f"Error listing database tables: {e}")
                return None
        else:
            print("Error listing database tables: Engine not initialized.")
            return None

# Initialize DatabaseConnector instance
connector = DatabaseConnector("db_creds.yaml")

# Call the list_db_tables method
connector.list_db_tables()
