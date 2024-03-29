import psycopg2
import yaml
from sqlalchemy import create_engine, MetaData

class DatabaseConnector:
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None

    def connect(self):
        """
        Connect to the PostgreSQL database.
        """
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print("Connected to the database successfully.")
        except psycopg2.Error as e:
            print(f"Error connecting to the database: {e}")

    def upload_to_db(self, df, table_name):
        """
        Uploads a pandas DataFrame to a PostgreSQL database table.

        Args:
        - df (pandas DataFrame): DataFrame containing the data to upload.
        - table_name (str): Name of the table to upload data to.
        """
        try:
            # Initialize database engine
            engine = self.init_db_engine("db_creds.yaml")
            
            # Upload DataFrame to database table
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            
            print(f"Data uploaded to table '{table_name}' successfully.")
        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")

    def disconnect(self):
        """
        Disconnect from the PostgreSQL database.
        """
        if self.connection:
            self.connection.close()
            print("Disconnected from the database.")

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
                db_url = f"postgresql://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}"
                
                # Create engine
                engine = create_engine(db_url)
                return engine
            except Exception as e:
                print(f"Error initializing database engine: {e}")
                return None
        else:
            print("Error initializing database engine: Credentials not found.")
            return None

    @staticmethod
    def list_db_tables(creds_file):
        """
        List all tables in the database.

        Args:
        - creds_file (str): Path to the YAML file containing credentials.

        Returns:
        - list: List of table names.
        """
        engine = DatabaseConnector.init_db_engine(creds_file)
        if engine:
            try:
                # Reflect database metadata
                meta = MetaData()
                meta.reflect(bind=engine)

                # Get table names
                table_names = meta.tables.keys()
                return table_names
            except Exception as e:
                print(f"Error listing database tables: {e}")
                return None
        else:
            print("Error listing database tables: Engine not initialized.")
            return None