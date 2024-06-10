import json
import mysql.connector
import os
from dotenv import load_dotenv
open_fda_cols= ["manufacturer_name","substance_name","route"]

counter = 0

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
        result = data.get('results', [])
        
        cursor.execute("SHOW TABLES LIKE 'otc_medication'")
        table_exists = cursor.fetchone()
        
        cursor.execute("DESC otc_medication")
        column_names = [row[0] for row in cursor.fetchall()]
        # print(column_names)
        
        for drug_data in result:
            
            # Safely access 'openfda' and 'product_type' using get
            openfda_data = drug_data.get('openfda', {})
            product_type = openfda_data.get('product_type')
            
            # Check if 'product_type' exists and is a non-empty list
            if product_type and product_type[0] == "HUMAN PRESCRIPTION DRUG":
                counter +=1
                insert_query = "INSERT INTO otc_medication (manufacturer_name, substance_name, route) VALUES (%s, %s, %s)"
                cursor.execute(insert_query, (openfda_data.get('manufacturer_name', [''])[0], 
                                openfda_data.get('substance_name', [''])[0], 
                                openfda_data.get('route', [''])[0]))    
                conn.commit()      
            else:
                continue 
    print(counter)
except mysql.connector.Error as err:
    print(err)
else:
  conn.close()