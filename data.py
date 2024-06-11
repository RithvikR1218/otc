import json
import mysql.connector
import os
from dotenv import load_dotenv

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
        
        for drug_data in result:
            
            openfda_data = drug_data.get('openfda', {})
            product_type = openfda_data.get('product_type')
            
            # Check if 'product_type' exists and is a non-empty list
            if product_type and product_type[0] == "HUMAN OTC DRUG":
                counter +=1
                
                purpose = drug_data.get('purpose', [''])[0]
                if not purpose:
                    purpose = 'MISSING'
                brand_name = openfda_data.get('brand_name', [''])[0]
                if not brand_name:
                    brand_name = 'MISSING'
                generic_name = openfda_data.get('generic_name', [''])[0]
                if not generic_name:
                    generic_name = 'MISSING'
                manufacturer_name = openfda_data.get('manufacturer_name', [''])[0]
                if not manufacturer_name:
                    manufacturer_name = 'MISSING'
                active_ingredient = drug_data.get('active_ingredient', [''])[0]
                if not active_ingredient:
                    active_ingredient = 'MISSING'

                # For substance_name, append multiple values together if they exist
                substance_name_list = openfda_data.get('substance_name', [])
                if substance_name_list:
                    substance_name = ', '.join(substance_name_list)
                else:
                    substance_name = 'MISSING'
                
                insert_query = "INSERT INTO otc_medications (purpose, brand_name, generic_name,manufacturer_name,active_ingredient,substance_name) VALUES (%s, %s, %s,%s, %s, %s)"
                cursor.execute(insert_query, (purpose, brand_name,generic_name,manufacturer_name,active_ingredient,substance_name))    
                conn.commit()      
            else:
                continue 
    print(counter)
except mysql.connector.Error as err:
    print(err)
else:
  conn.close()