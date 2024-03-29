from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import yaml

if __name__ == "__main__":
    creds_file = "db_creds.yaml"

    # Read database credentials from YAML file
    creds = DatabaseConnector.read_db_creds(creds_file)

    if creds:
        # Initialize DatabaseConnector object with credentials
        connector = DatabaseConnector(
            dbname=creds['RDS_DATABASE'],
            user=creds['RDS_USER'],
            password=creds['RDS_PASSWORD'],
            host=creds['RDS_HOST'],
            port=creds['RDS_PORT']
        )

        # Connect to the database
        connector.connect()

        # Initialize DataExtractor object
        extractor = DataExtractor()

        # Extract data from the RDS database
        user_data_table_name = "aicore_admin"  # Assuming the table name containing user data
        user_data_df = extractor.read_rds_table(connector, user_data_table_name)

        # Clean the extracted data
        cleaned_user_data = DataCleaning().clean_user_data(user_data_df)

        # Upload cleaned data to the dim_users table
        dim_users_table_name = "dim_users"
        connector.upload_to_db(cleaned_user_data, dim_users_table_name)

        # Disconnect from the database
        connector.disconnect()
    else:
        print("Error reading database credentials from YAML file.")
