
import pandas as pd
import pyodbc
import sqlite3
import copy
import datetime

strings = {'servername': r'LAPTOP-EQ5HHEML\SQLEXPRESS',
      'database': 'datawarehouse',
      'password': '',
      'username': r'LAPTOP-EQ5HHEML\itsum'}

conn_str = (
    "DRIVER={SQL Server};Server=" + strings["servername"] + 
    ";Database=" + strings["database"] + 
    ";User ID=" + strings["username"] + 
    ";Password=" + strings["password"] + 
    "trusted_connection=yes;"
)

export_conn = pyodbc.connect(conn_str)
export_cursnor = export_conn.cursor()

crm_conn = sqlite3.connect("../week 2/go_crm.sqlite")
sales_conn = sqlite3.connect("../week 2/go_sales.sqlite")
staff_conn = sqlite3.connect("../week 2/go_staff.sqlite")

# tabel

product = pd.read_sql("SELECT * FROM product", sales_conn)
sales_staff = pd.read_sql("SELECT * FROM sales_staff", staff_conn)
return_reason = pd.read_sql("SELECT * FROM return_reason", sales_conn)
course = pd.read_sql("SELECT * FROM course", staff_conn)

# retailer inlezen

retailer_site = pd.read_sql("SELECT * FROM retailer_site", crm_conn)
retailer_contact = pd.read_sql("SELECT * FROM retailer_contact", crm_conn)
retailer_segment = pd.read_sql("SELECT * FROM retailer_segment", crm_conn)
retailer_type = pd.read_sql("SELECT * FROM retailer_type", crm_conn)
age_group = pd.read_sql("SELECT * FROM age_group", crm_conn)
retailer = pd.read_sql("SELECT * FROM retailer", crm_conn)
retailer_headquarters = pd.read_sql("SELECT * FROM retailer_headquarters", crm_conn)
sales_demographic = pd.read_sql("SELECT * FROM sales_demographic", crm_conn)

#retailer merging

retailer_site_contact = pd.merge(retailer_site, retailer_contact, on='RETAILER_SITE_CODE')
retailer_site_contact_retailer = pd.merge(retailer_site_contact, retailer, on='RETAILER_CODE')
retailer_site_contact_type = pd.merge(retailer_site_contact_retailer, retailer_type, on='RETAILER_TYPE_CODE')
segment_headquarters = pd.merge(retailer_segment, retailer_headquarters, on="SEGMENT_CODE")
retailer_site_contact_type_sh = pd.merge(segment_headquarters, retailer_site_contact_type, on="RETAILER_CODEMR")
age_sales_demo = pd.merge(age_group, sales_demographic, on="AGE_GROUP_CODE")
retailer_site_contact_type_sh_age = pd.merge(retailer_site_contact_type_sh, age_sales_demo, on="RETAILER_CODEMR")

for index, row in retailer_site_contact_type_sh_age.iterrows():
    try:
        query = "INSERT INTO retailer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" 
        values = (
            row['RETAILER_SITE_CODE'],
            row['RETAILER_CONTACT_CODE'],
            row['FIRST_NAME'],
            row['LAST_NAME'],
            row['EXTENSION'],
            row['RETAILER_CODEMR'],
            row['COMPANY_NAME'],
            row['POSTAL_ZONE_x'],
            row['REGION_x'],
            row['CITY_x'],
            row['RETAILER_TYPE_EN'],
            row['RETAILER_TYPE_CODE'],
            row['COUNTRY_CODE_x'],
            row['SEGMENT_CODE'],
            row['GENDER'],
            row['AGE_GROUP_CODE']
        )         
        export_cursnor.execute(query, *values)
    except pyodbc.Error as e:
        print(f"Error inserting row {index}: {e}")
        print(query, values)

export_conn.commit()
export_cursnor.close()

# Creating a slowly changing dimension (assuming type 2) for every dimension in the datawarehouse
for name in ["product", "sales_staff", "return_reason", "course"]:
    # Copy
    globals()[name + "_scd"] = copy.deepcopy(globals()[name])

    # Add columns
    globals()[name + "_scd"]["Nummer_sk"] = range(len(globals()[name + "_scd"]))
    globals()[name + "_scd"]["Timestamp"] = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M")

    # Fix index
    globals()[name + "_scd"].set_index("Nummer_sk", inplace=True)