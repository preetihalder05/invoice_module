from flask import Flask,jsonify,request
import pandas as pd
# from sqlalchemy import create_engine
import mysql.connector
import json
# pip install -r requirements.txt

app = Flask(__name__)
with open('config.json','r') as c:
    params=json.load(c)['params']


config = {
    'user': params['user'],
    'password': params['password'],
    'host': params['host'],
    'database': params['database']
}
@app.route('/')
def index():
    return "hello"

@app.route('/fetch_data',methods=['GET'])
def fetch_data():
    df=pd.read_csv('preeti.csv')
    # df.head()
    # Convert the DataFrame to JSON format
    json_data = df.to_json(orient='records', lines=True)

    # Save JSON data to a file (optional)
    with open('preeti.json', 'w') as file:
        file.write(json_data)

    # Convert the DataFrame to a list of dictionaries
    list_of_dicts = df.to_dict(orient='records')

    # Print only the first 5 dictionaries
    for record in list_of_dicts[:5]:
        print(record)
    # Print the JSON data (optional)
    # print(type(json_data))
    #print(json_data)
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(**config)
        # print("Connection Success")
        cursor = conn.cursor()

        # Insert the user data without specifying user_id
        df.to_sql('preeti_data', if_exists='replace', index=False)
        print("Data imported successfully into MySQL database!")
        # print("Data Inserted")
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
        return {"error": "An error occurred", "message": str(e)}
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
    # return jsonify({})
    return "data inserted"







































# # Step 1: Read CSV and Convert to JSON
# df = pd.read_csv('preeti.csv')

# # Optional: Save JSON data if needed
# df.to_json('preeti.json', orient='records', lines=True)

# Step 2: Connect to MySQL Database
# db_user = 'root'          # Replace with your MySQL username
# db_password = 'root'  # Replace with your MySQL password
# db_host = 'localhost'          # Replace with your database host
# db_name = 'preeti_db'          # Your MySQL database name

# engine = create_engine(f'mysql+mysqlconnector://root:root@{db_host}/preeti_db')




































# # Step 3: Import CSV Data into MySQL Table
# table_name = 'preeti_data'  # Ensure this table exists in your MySQL database



# try:
#     # Dynamically create table and insert data
#     df.to_sql('preeti_data', con=engine, if_exists='replace', index=False)
#     print("Data imported successfully into MySQL database!")
# except Exception as e:
#     print("Error:", e)

# # Optional: Verify data insertion
# with engine.connect() as connection:
#     result = connection.execute(f"SELECT COUNT(*) FROM {'preeti_data'}")
#     for row in result:
#         print("Number of records inserted:", row[0])

if __name__ == "__main__":
    app.run(debug=True)