import pandas as pandass
import pyodbc as pjotrdbc
import sqlite3 as sqlite987254548547664678626861876587265475682465
import copy as koppie
import datetime as gaytime

strings = {'servername': r'LAPTOP-FTPRSG8G\SQLEXPRESS02',
      'database': 'datawarehouse',
      'password': '',
      'username': r'LAPTOP-FTPRSG8G\Will'}

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

# tabellen inlezen

product = pandass.read_sql("SELECT * FROM product", sales_conn)
sales_staff = pandass.read_sql("SELECT * FROM sales_staff", staff_conn)
return_reason = pandass.read_sql("SELECT * FROM return_reason", sales_conn)
course = pandass.read_sql("SELECT * FROM course", staff_conn)
product_type = pandass.read_sql("SELECT * FROM product_type", sales_conn)
product_line = pandass.read_sql("SELECT * FROM product_line", sales_conn)
sales_branch = pandass.read_sql("SELECT * FROM sales_branch", sales_conn)
return_reason = pandass.read_sql("SELECT * FROM return_reason", sales_conn)
course = pandass.read_sql("SELECT * FROM course", staff_conn)

# retailer merge tabellen inlezen

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

#product merging

product_type_line = pandass.merge(product_line, product_type, on="PRODUCT_LINE_CODE")
product_df = pandass.merge(product, product_type_line, on="PRODUCT_TYPE_CODE")

#sales_staff merging

sales_staff_df = pandass.merge(sales_staff, sales_branch, on="SALES_BRANCH_CODE")

def format_date(date_str):
    date_obj = gaytime.datetime.strptime(date_str, '%d-%m-%Y')
    formatted_date = date_obj.strftime('%Y-%m-%d')
    return formatted_date

for index, row in retailer_df.iterrows():
    try:
        query = "INSERT INTO retailer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" 
        values = (
            row['RETAILER_SITE_CODE'],
            row['RETAILER_CODEMR'],
            row['COMPANY_NAME'],
            row['POSTAL_ZONE_x'],
            row['REGION_x'],
            row['CITY_x'],
            row['RETAILER_TYPE_EN'],
            row['RETAILER_TYPE_CODE'],
            row['COUNTRY_CODE_x'],
            row['COUNTRY_EN'],
            row['SEGMENT_CODE'],
            row['SEGMENT_NAME'],
            row['GENDER'],
            row['AGE_GROUP_CODE'],
            row['UPPER_AGE'],
            row['LOWER_AGE'],
            row['SALES_TERRITORY_CODE'],
            row['TERRITORY_NAME_EN'],
            row['SALES_PERCENT']
        )         
        export_cursnor.execute(query, *values)
    except pjotrdbc.Error as e:
        print(f"Error inserting row {index}: {e}")
        print(query, values)

for index, row in product_df.iterrows():
    try:
        sales_price = float(row['PRODUCTION_COST']) * (1 + float(row['MARGIN']))
        formatted_date = format_date(row['INTRODUCTION_DATE'])
        query = "INSERT INTO product VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" 
        values = (
            row['PRODUCT_NAME'],
            row['DESCRIPTION'],
            sales_price,
            row['LANGUAGE'],
            row['PRODUCTION_COST'],
            row['MARGIN'],
            formatted_date,
            row['PRODUCT_TYPE_EN'],
            row['PRODUCT_LINE_EN'],
            row['PRODUCT_LINE_CODE']
        )         
        export_cursnor.execute(query, *values)
    except pjotrdbc.Error as e:
        print(f"Error inserting row {index}: {e}")
        print(query, values)
    
for index, row in sales_staff_df.iterrows():
    try:
        query = "INSERT INTO sales_staff VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" 
        values = (
            row['FIRST_NAME'],
            row['LAST_NAME'],
            row['EXTENSION'],
            row['WORK_PHONE'],
            row['FAX'],
            row['EMAIL'],
            row['DATE_HIRED'],
            row['ADDRESS1'],
            row['ADDRESS2']
        )         
        export_cursnor.execute(query, *values)
    except pjotrdbc.Error as e:
        print(f"Error inserting row {index}: {e}")
        print(query, values)

for index, row in return_reason.iterrows():
    try:
        query = "INSERT INTO return_reason VALUES (?)" 
        values = (
            (row['RETURN_DESCRIPTION_EN'],)
        )         
        export_cursnor.execute(query, *values)
    except pjotrdbc.Error as e:
        print(f"Error inserting row {index}: {e}")
        print(query, values)

for index, row in course.iterrows():
    try:
        query = "INSERT INTO course VALUES (?)" 
        values = (
            (row['COURSE_DESCRIPTION'],)
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
