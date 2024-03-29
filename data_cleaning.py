import pandas as pd

class DataCleaning:
    def __init__(self):
        pass

    def clean_csv_data(self, file_path):
        """
        Clean data from a CSV file.

        Args:
        - file_path (str): Path to the CSV file.

        Returns:
        - pandas DataFrame: Cleaned data.
        """
        try:
            # Load CSV data into a DataFrame
            df = pd.read_csv(file_path)

            # Perform data cleaning operations
            # Example: Remove duplicates
            cleaned_data = df.drop_duplicates()

            return cleaned_data
        except FileNotFoundError:
            print("File not found.")
            return None

    def clean_api_data(self, api_data):
        """
        Clean data obtained from an API.

        Args:
        - api_data (list of dict): Data obtained from the API.

        Returns:
        - pandas DataFrame: Cleaned data.
        """
        try:
            # Convert API data to DataFrame
            df = pd.DataFrame(api_data)

            # Perform data cleaning operations
            # Example: Convert date strings to datetime objects
            df['date'] = pd.to_datetime(df['date'])

            return df
        except Exception as e:
            print(f"Error cleaning API data: {e}")
            return None

    def clean_s3_data(self, s3_data):
        """
        Clean data obtained from an S3 bucket.

        Args:
        - s3_data (list of str): Data obtained from the S3 bucket.

        Returns:
        - pandas DataFrame: Cleaned data.
        """
        try:
            # Convert S3 data to DataFrame
            df = pd.DataFrame(s3_data)

            # Perform data cleaning operations
            # Example: Drop rows with missing values
            cleaned_data = df.dropna()

            return cleaned_data
        except Exception as e:
            print(f"Error cleaning S3 data: {e}")
            return None
        
    def clean_user_data(self, user_data_df):
        """
        Cleans the user data DataFrame.

        Args:
        - user_data_df (pandas DataFrame): DataFrame containing user data.

        Returns:
        - pandas DataFrame: DataFrame with cleaned user data.
        """
        cleaned_df = user_data_df.copy()  # Make a copy of the original DataFrame
        
        # Drop rows with NULL values
        cleaned_df = cleaned_df.dropna()

        # Convert date columns to datetime type, handle errors with dates
        date_columns = ['date_of_birth', 'registration_date']
        for col in date_columns:
            if col in cleaned_df.columns:
                try:
                    cleaned_df[col] = pd.to_datetime(cleaned_df[col])
                except ValueError as e:
                    print(f"Error converting {col} to datetime: {e}")
                    # You can handle errors here, e.g., setting the problematic dates to NaN
            else:
                print(f"Column '{col}' not found in DataFrame.")

        # Convert columns to the correct data type
        # Example: cleaned_df['age'] = cleaned_df['age'].astype(int)

        # Perform additional cleaning as needed

        return cleaned_df

# Example usage:
if __name__ == "__main__":
    # Instantiate the DataCleaning class
    cleaner = DataCleaning()

    # Example CSV data cleaning
    csv_file_path = "example.csv"
    cleaned_csv_data = cleaner.clean_csv_data(csv_file_path)
    print("Cleaned CSV data:")
    print(cleaned_csv_data)

    # Example API data cleaning
    api_data = [{"id": 1, "name": "Product A", "price": 100, "date": "2022-01-01"},
                {"id": 2, "name": "Product B", "price": 200, "date": "2022-01-02"}]
    cleaned_api_data = cleaner.clean_api_data(api_data)
    print("Cleaned API data:")
    print(cleaned_api_data)

    # Example S3 data cleaning
    s3_data = ["data1", "data2", "data3", None, "data4"]
    cleaned_s3_data = cleaner.clean_s3_data(s3_data)
    print("Cleaned S3 data:")
    print(cleaned_s3_data)
