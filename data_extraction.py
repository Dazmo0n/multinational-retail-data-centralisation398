import psycopg2
import boto3
import csv
import requests
from sqlalchemy import create_engine
import pandas as pd
from database_utils import DatabaseConnector
import yaml

class DataExtractor:
    def __init__(self):
        pass

    def extract_from_csv(self, file_path):
        """
        Extracts data from a CSV file.

        Args:
        - file_path (str): Path to the CSV file.

        Returns:
        - list: List of dictionaries containing the extracted data.
        """
        extracted_data = []
        try:
            with open(file_path, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    extracted_data.append(dict(row))
        except FileNotFoundError:
            print("File not found.")
        return extracted_data

    def extract_from_api(self, url):
        """
        Extracts data from an API.

        Args:
        - url (str): URL of the API.

        Returns:
        - list: List of dictionaries containing the extracted data.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            extracted_data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")
            extracted_data = []
        return extracted_data

    def extract_from_s3(self, bucket_name, object_key):
        """
        Extracts data from an S3 bucket.

        Args:
        - bucket_name (str): Name of the S3 bucket.
        - object_key (str): Key of the object in the S3 bucket.

        Returns:
        - list: List of dictionaries containing the extracted data.
        """
        extracted_data = []
        try:
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            data = response['Body'].read().decode('utf-8')
            for line in data.split('\n'):
                extracted_data.append(line.split(','))
        except Exception as e:
            print(f"Error fetching data from S3: {e}")
        return extracted_data

    def extract_from_rds(self, creds_file, query):
        """
        Extracts data from an RDS database.

        Args:
        - creds_file (str): Path to the YAML file containing database credentials.
        - query (str): SQL query to fetch data from the database.

        Returns:
        - list: List of dictionaries containing the extracted data.
        """
        try:
            # Initialize database engine
            engine = self._init_db_engine(creds_file)

            # Execute SQL query
            with engine.connect() as connection:
                result = connection.execute(query)
                extracted_data = [dict(row) for row in result]
            
            return extracted_data
        except Exception as e:
            print(f"Error fetching data from RDS: {e}")
            return []


