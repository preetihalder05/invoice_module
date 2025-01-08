from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine
import json
import mysql.connector
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

config={
    'user' : params['user'],
    'password' : params['password'],
    'host' : params['host'],
    'database' : params['database']
}

@app2.route('/')
def index():
    return "Hello, Flask is running!"


@app2.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        # Step 1: Read CSV Data
        df = pd.read_csv('preeti.csv')
        json_data = df.to_json(orient='records', lines=True)
        
        # Optional: Save JSON to a file
        with open('preeti.json', 'w') as file:
            file.write(json_data)

     
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
    # return "data inserted into database"

@app2.route('/fetch_data',methods=['GET'])
def fetch_data():
    # print('hhi')
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(**config)
        print("Connection Success")
        cursor = conn.cursor(dictionary=True)

        
        select_query = "select * from preeti_data;"
        cursor.execute(select_query)
        user = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        if user:
            return jsonify(user)
        else:
            return jsonify({ "message": "No user found with the provided page_no or entry-per-page"})

    except mysql.connector.Error as err:
        return jsonify({"error": "An error occurred", "message": str(err)}) 

if __name__ == "__main__":
    app2.run(debug=True)
