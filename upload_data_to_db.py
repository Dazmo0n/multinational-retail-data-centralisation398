from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

if __name__ == "__main__":

    creds_file = "db_creds.yaml"
    
    # Initialize DataExtractor and DatabaseConnector objects
    extractor = DataExtractor()
    connector = DatabaseConnector(dbname='sales_data', user='pagila', password='mouwril2468')

    # Extract data from the RDS database
    user_data_table_name = "aicore_admin"  # Assuming the table name containing user data
    user_data_df = extractor.read_rds_table(connector, user_data_table_name)

    # Clean the extracted data
    cleaned_user_data = DataCleaning().clean_user_data(user_data_df)

    # Upload cleaned data to the dim_users table
    dim_users_table_name = "dim_users"
    connector.upload_to_db(cleaned_user_data, dim_users_table_name)
