from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine
import json
import boto3
import io
import mysql.connector
import os

# Initialize Flask app
app2 = Flask(__name__)

# Load configuration from JSON
with open('config.json', 'r') as c:
    params = json.load(c)['params']

# Database Configuration
db_user = params['user']
db_password = params['password']
db_host = params['host']
db_name = params['database']

# SQLAlchemy Engine
engine = create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}")

config = {
    'user': params['user'],
    'password': params['password'],
    'host': params['host'],
    'database': params['database']
}

# S3 Configuration
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_INVOICE'),
    aws_secret_access_key=os.getenv('AWS_SECRECT_INVOICE'),
    region_name=os.getenv('AWS_REGION_INVOICE')  # Replace with your AWS region
)

@app2.route('/')
def index():
    return str(os.getenv('AWS_REGION_INVOICE'))

@app2.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        # Step 1: Read CSV file from S3
        bucket_name = 'invoicing-module'
        s3_key = 'issuer_data/FX Raw Data 26thJune24 to 30th June24.csv'

        # Download the CSV file content into memory
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')

        # Convert the CSV content to a DataFrame
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Convert DataFrame to JSON
        json_data = df.to_json(orient='records', lines=True)

        # Optional: Save JSON to a local file
        with open('preeti.json', 'w') as file:
            file.write(json_data)

        # Print a few records to verify
        list_of_dicts = df.to_dict(orient='records')
        for record in list_of_dicts[:5]:
            print(record)

        # Step 2: Insert Data into MySQL
        df.to_sql('preeti_data', con=engine, if_exists='replace', index=False)
        print("Data imported successfully into MySQL database!")

        return jsonify({"message": "Data inserted successfully!"})

    except Exception as e:
        print("Error:", e)
        return jsonify({"error": "An error occurred", "message": str(e)})

    finally:
        engine.dispose()

@app2.route('/fetch_data', methods=['GET'])
def fetch_data():
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(**config)
        print("Connection Success")
        cursor = conn.cursor(dictionary=True)

        # Fetch data
        select_query = "SELECT * FROM preeti_data;"
        cursor.execute(select_query)
        user = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        if user:
            return jsonify(user)
        else:
            return jsonify({"message": "No data found in the table"})

    except mysql.connector.Error as err:
        return jsonify({"error": "An error occurred", "message": str(err)})

if __name__ == "__main__":
    app2.run(debug=True)
