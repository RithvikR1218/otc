import json
import mysql.connector
import os
from dotenv import load_dotenv

def extract_field_names(data, parent_key=''):
    field_names = set()
    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            field_names.add(full_key)
            field_names.update(extract_field_names(value, full_key))
    elif isinstance(data, list):
        for item in data:
            field_names.update(extract_field_names(item, parent_key))

    return field_names

def check_and_create_table(cursor, field_names):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'otc_medication'
    """)
    
    if cursor.fetchone()[0] == 0:
        # Table does not exist, create it
        columns = ", ".join([f"`{name.replace('.', '*')}` VARCHAR(50)" for name in field_names])
        create_table_query = f"CREATE TABLE otc_medication ({columns})"
        cursor.execute(create_table_query)
        print("Table `otc_medication` created.")
    else:
        print("Table `otc_medication` already exists.")

load_dotenv()
config = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USERNAME"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_DATABASE"),
    'raise_on_warnings': True
}

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    with open('../extra/drug-label-0001-of-0012.json', 'r') as file:
        data = json.load(file)
        
        # Safely access 'results' using get
        result = data.get('results', [])
        
        # Collect field names
        all_field_names = set()
        for drug_data in result:
            all_field_names.update(extract_field_names(drug_data))
            # break
                
        # Check if table exists and create if not
        check_and_create_table(cursor, all_field_names)
            
except mysql.connector.Error as err:
    print(err)
else:
    conn.close()
