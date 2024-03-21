import pandas as pandass
import pyodbc as pjotrdbc
import sqlite3 as sqlite987254548547664678626861876587265475682465
import copy as koppie
import datetime as gaytime

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

export_conn = pjotrdbc.connect(conn_str)
export_cursnor = export_conn.cursor()

crm_conn = sqlite987254548547664678626861876587265475682465.connect("../week 2/go_crm.sqlite")
sales_conn = sqlite987254548547664678626861876587265475682465.connect("../week 2/go_sales.sqlite")
staff_conn = sqlite987254548547664678626861876587265475682465.connect("../week 2/go_staff.sqlite")

# tabel

product = pandass.read_sql("SELECT * FROM product", sales_conn)
sales_staff = pandass.read_sql("SELECT * FROM sales_staff", staff_conn)
return_reason = pandass.read_sql("SELECT * FROM return_reason", sales_conn)
course = pandass.read_sql("SELECT * FROM course", staff_conn)

# retailer inlezen

retailer_site = pandass.read_sql("SELECT * FROM retailer_site", crm_conn)
retailer_contact = pandass.read_sql("SELECT * FROM retailer_contact", crm_conn)
retailer_segment = pandass.read_sql("SELECT * FROM retailer_segment", crm_conn)
retailer_type = pandass.read_sql("SELECT * FROM retailer_type", crm_conn)
age_group = pandass.read_sql("SELECT * FROM age_group", crm_conn)
retailer = pandass.read_sql("SELECT * FROM retailer", crm_conn)
retailer_headquarters = pandass.read_sql("SELECT * FROM retailer_headquarters", crm_conn)
sales_demographic = pandass.read_sql("SELECT * FROM sales_demographic", crm_conn)
country = pandass.read_sql("SELECT * FROM country", crm_conn)
sales_territory = pandass.read_sql("SELECT * FROM sales_territory", crm_conn)

#retailer merging

retailer_site_contact = pandass.merge(retailer_site, retailer_contact, on='RETAILER_SITE_CODE')
retailer_site_contact_retailer = pandass.merge(retailer_site_contact, retailer, on='RETAILER_CODE')
retailer_site_contact_type = pandass.merge(retailer_site_contact_retailer, retailer_type, on='RETAILER_TYPE_CODE')
segment_headquarters = pandass.merge(retailer_segment, retailer_headquarters, on="SEGMENT_CODE")
retailer_site_contact_type_sh = pandass.merge(segment_headquarters, retailer_site_contact_type, on="RETAILER_CODEMR")
age_sales_demo = pandass.merge(age_group, sales_demographic, on="AGE_GROUP_CODE")
retailer_site_contact_type_sh_age = pandass.merge(retailer_site_contact_type_sh, age_sales_demo, on="RETAILER_CODEMR")
retailer_site_contact_type_sh_age_country = pandass.merge(retailer_site_contact_type_sh_age, country, left_on="COUNTRY_CODE_x", right_on="COUNTRY_CODE")
retailer_df = pandass.merge(retailer_site_contact_type_sh_age_country, sales_territory, on="SALES_TERRITORY_CODE")

for index, row in retailer_df.iterrows():
    try:
        query = "INSERT INTO retailer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" 
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
            row['AGE_GROUP_CODE'],
            row['COUNTRY_EN'],
            row['SALES_TERRITORY_CODE'],
            row['TERRITORY_NAME_EN']
        )         
        export_cursnor.execute(query, *values)
    except pjotrdbc.Error as e:
        print(f"Error inserting row {index}: {e}")
        print(query, values)

export_conn.commit()
export_cursnor.close()

# Creating a slowly changing dimension (assuming type 2) for every dimension in the datawarehouse
for name in ["product", "sales_staff", "return_reason", "course"]:
    # Copy
    globals()[name + "_scd"] = koppie.deepcopy(globals()[name])

    # Add columns
    globals()[name + "_scd"]["Nummer_sk"] = range(len(globals()[name + "_scd"]))
    globals()[name + "_scd"]["Timestamp"] = gaytime.datetime.now().strftime("%d/%m/%Y, %H:%M")

    # Fix index
    globals()[name + "_scd"].set_index("Nummer_sk", inplace=True)
