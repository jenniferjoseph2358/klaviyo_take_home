# Data Exercise | Database Design | Klaviyo
# Jennifer Joseph

# Question 5: Write a python script to check if this file exists in a cloud storage provider of your choice
# and then load into a table in a database / data warehouse

# Cloud Storage Provider = AWS s3
# Database / Data warehouse = Snowflake

import pandas as pd
import snowflake.connector as sf
import boto3
import botocore
from snowflake.connector.pandas_tools import write_pandas

# FUTURE TO DO LIST -- listing out future improvements and edge cases not yet accounted for

# TODO: HANDLE DUPLICATES
## in invoices, invoice 2 is duplicated
## in customers, id 16 is "duplicated" but that customer has differing info for billing state

# TODO: SQL FUNCTION FOR DDL
## function that accepts the table name as a string, and the column names and data types as a dictionary
## would need to be able to pass constraints for column definitions as well

# TODO: ITEMS ID
## composite key of information from invoices if nothing else

# TODO: Expand functionality of check_if_exists() so it can accept multiple keys to look for in one call

# TODO: Is there a better way to DRY with defining column names?

#TODO: Complete s3_to_snowflake() 

# CONFIGURE AWS AND SNOWFLAKE CREDENTIALS -- removed variable values for submission

# AWS

service_name = 's3'
region_name = ''
aws_access_key_id = ''
aws_secret_access_key = ''

my_bucket = 'myawsbucket'
invoices_key = 'invoices.csv'
fake_invoices_key = 'j_invoices.csv'

s3 = boto3.resource(
    service_name=service_name,
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# SNOWFLAKE

user = ''
password = ''
account = ''
database = '' # created in snowflake GUI using + Database button
warehouse = ''
schema = ''
role = ''

conn = sf.connect(user=user,password=password,account=account, database=database, warehouse=warehouse, schema=schema, role=role);

# Creating a few SQL statements as variables
# for following DDL variables, see TODO

# CREATING TABLE TO STORE INVOICE DATA IN ITS ORIGINAL FORMAT

create_table_statement = "CREATE OR REPLACE TABLE test_invoice(" \
                         "INVOICEID integer," \
                         "INVOICEDATE date," \
                         "CUSTOMERID integer," \
                         "CUSTOMER string," \
                         "BILLINGADDRESS string," \
                         "BILLINGCITY string," \
                         "BILLINGSTATE string," \
                         "BILLINGCOUNTRY string," \
                         "BILLINGPOSTALCODE string," \
                         "ITEM1 string," \
                         "PRICE1 float,"\
                         "ITEM2 string," \
                         "PRICE2 float,"\
                         "ITEM3 string," \
                         "PRICE3 float," \
                         "ITEM4 string,"\
                         "PRICE4 float,"\
                         "ITEM5 string,"\
                         "PRICE5 float, "\
                         "ITEM6 string," \
                         "PRICE6 float,"\
                         "ITEM7 string, "\
                         "PRICE7 float," \
                         "ITEM8 string," \
                         "PRICE8 float," \
                         "ITEM9 string, "\
                         "PRICE9 float," \
                         "ITEM10 string," \
                         "PRICE10 float,"\
                         "ITEM11 string," \
                         "PRICE11 float," \
                         "ITEM12 string," \
                         "PRICE12 float," \
                         "ITEM13 string," \
                         "PRICE13 float," \
                         "ITEM14 string," \
                         "PRICE14 float," \
                         "ITEM15 string," \
                         "PRICE15 float)"

# CREATING TABLES TO STORE INVOICE DATA BY DESIGN PROPOSED IN TAKE HOME WRITE UP

create_customers_table_statement = "CREATE OR REPLACE TABLE customers (" \
                         "CUSTOMERID integer," \
                         "CUSTOMER string," \
                         "BILLINGADDRESS string," \
                         "BILLINGCITY string," \
                         "BILLINGSTATE string," \
                         "BILLINGCOUNTRY string," \
                         "BILLINGPOSTALCODE string)"

create_invoices_table_statement = "CREATE OR REPLACE TABLE invoices (" \
                         "INVOICEID integer," \
                         "INVOICEDATE date," \
                         "CUSTOMERID integer)"

create_items_table_statement = "CREATE OR REPLACE TABLE items (" \
                         "INVOICEID integer," \
                         "ITEM string," \
                         "PRICE float)"

# Functions defined for exercise

def run_query(conn, query):
    cursor = conn.cursor();
    cursor.execute(query);
    cursor.close();

def check_for_file(bucket_name, key):
    try:
        s3.Object(bucket_name, key).load()
        print(f"File {key} exists in bucket {bucket_name}.")
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"File {key} does not exist in bucket {bucket_name}.")
            return False
        else:
            raise

# ideally we would have a function to complete this entire pipeline, but there are a few pieces to this not yet solved, so leaving as attempted here 

def s3_to_snowflake(bucket_name, key, conn, table_name, columns_for_table):
    if check_for_file(bucket_name, key):
        object_from_s3 = s3.Object(bucket_name, key)
        df = pd.read_csv(object_from_s3.get()['Body'])
        df_for_table = df[columns_for_table] ## these would need to match / we would need to match the subset input via the columns_for_table value
        write_pandas(conn, df_for_table, table_name) ## you would have to know that the table already exists with the specified columns / the right data types and not have to apply any transformations, or create the table from the df columns 
        print(f'{df_for_table.shape[0]} rows and {df_for_table.shape[1]} columns have been loaded into {table_name} in Snowflake.')
    else:
        return check_for_file(bucket_name, key)

# Execute SQL commands

run_query(conn,create_table_statement) # original structure
run_query(conn,create_customers_table_statement) # proposed structure
run_query(conn,create_invoices_table_statement)
run_query(conn,create_items_table_statement)

# STEP 1 (of Q#5): CHECK IF FILE EXISTS

file_exists = check_for_file(my_bucket, invoices_key) # output = File invoices.csv exists in bucket myawsbucket.
file_does_not_exist = check_for_file(my_bucket, fake_invoices_key) # output = File j_invoices.csv does not exist in bucket myawsbucket.

# STEP 2 (of Q#5): LOAD INTO SNOWFLAKE

invoices_from_s3_bucket = s3.Object(my_bucket,invoices_key)
df = pd.read_csv(invoices_from_s3_bucket.get()['Body'])
df.columns = [
    'INVOICEID',
    'INVOICEDATE',
    'CUSTOMERID',
    'CUSTOMER',
    'BILLINGADDRESS',
    'BILLINGCITY',
    'BILLINGSTATE',
    'BILLINGCOUNTRY',
    'BILLINGPOSTALCODE',
    'ITEM1',
    'PRICE1',
    'ITEM2',
    'PRICE2',
    'ITEM3',
    'PRICE3',
    'ITEM4',
    'PRICE4',
    'ITEM5',
    'PRICE5',
    'ITEM6',
    'PRICE6',
    'ITEM7',
    'PRICE7',
    'ITEM8',
    'PRICE8',
    'ITEM9',
    'PRICE9',
    'ITEM10',
    'PRICE10',
    'ITEM11',
    'PRICE11',
    'ITEM12',
    'PRICE12',
    'ITEM13',
    'PRICE13',
    'ITEM14',
    'PRICE14',
    'ITEM15',
    'PRICE15'
];

# CONVERT STRING COLUMN TO DATETIME -- above I defined the invoice date column as a string to initially read in the values, but need to convert
df['INVOICEDATE'] = pd.to_datetime(df['INVOICEDATE'], format='%m/%d/%y %H:%M')

# EXTRACT TO JUST DATE SINCE NO TIME INFORMATION -- minimal example of understanding upstream data definition - is there any significance to the midnight time on each invoice worth keeping?
df['INVOICEDATE'] = df['INVOICEDATE'].dt.date

write_pandas(conn,df,'TEST_INVOICE')
print(f'{df.shape[0]} rows and {df.shape[1]} columns have been loaded into TEST_INVOICE in Snowflake.')

# ENTITY TABLES -- defining dataframes for customers, invoices, and items to capture the properties that ONLY persist to those entities

df_customers = df[[
    'CUSTOMERID',
    'CUSTOMER',
    'BILLINGADDRESS',
    'BILLINGCITY',
    'BILLINGSTATE',
    'BILLINGCOUNTRY',
    'BILLINGPOSTALCODE'
]]

# as noted in my ERD, customers can have many invoices and thus we get duplicates when extracting customer info from invoices
## handling duplicates below, but getting a warning because of edge case mentioned in TODO section

print('Size of customers:', len(df_customers))
print('Shape of customers:', df_customers.shape)
print('drop dupes')

df_customers = df_customers[[
        'CUSTOMERID',
        'CUSTOMER',
        'BILLINGADDRESS',
        'BILLINGCITY',
        'BILLINGSTATE',
        'BILLINGCOUNTRY',
        'BILLINGPOSTALCODE'
]].drop_duplicates()
# note: the issue of ID vs. billing info here for uniqueness

print('Size of customers:', len(df_customers))
print('Shape of customers:', df_customers.shape)

# ADDITIONAL IMPROVEMENT -- instead of rewriting entire table, introduce staging table (or view) to store all records, and then only insert new records. Would need a way to refresh updates as well 

#run_query(conn,'TRUNCATE TABLE CUSTOMERS_STAGING')
#write_pandas(conn,df_customers, 'CUSTOMERS_STAGING')
#run_query(conn,"INSERT INTO MY_DATABASE.PUBLIC.CUSTOMERS select * from CUSTOMERS_STAGING WHERE CUSTOMERID not in (select CUSTOMERID from CUSTOMERS)")

write_pandas(conn,df_customers, 'CUSTOMERS')
print(f'{df_customers.shape[0]} rows and {df_customers.shape[1]} columns have been loaded into CUSTOMERS in Snowflake.')

# CREATING AN ITEMS TABLE, BY LOOPING THROUGH THE 15 PAIRS OF ITEM AND PRICE INFO COLUMNS

items = []
max_item_price_combo = 15

for i in range(1,max_item_price_combo + 1):
    item_col = f'ITEM{i}'
    price_col = f'PRICE{i}'

    for index, row in df.iterrows():
        invoice_id = row['INVOICEID']
        item = row[item_col]
        price = row[price_col]
        if pd.notna(item) and pd.notna(price): # recall that underlying data stores each item + price in a flat structure, and they are not necessarily all populated for each invoice
            items.append({'INVOICEID': invoice_id, 'ITEM': item, 'PRICE': price})

df_items = pd.DataFrame(items)
write_pandas(conn,df_items, 'ITEMS')
print(f'{df_items.shape[0]} rows and {df_items.shape[1]} columns have been loaded into ITEMS in Snowflake.')

# Finally, creating the invoices table from ERD

df_invoices = df[[
    'INVOICEID',
    'INVOICEDATE',
    'CUSTOMERID',
]]

write_pandas(conn,df_invoices, 'INVOICES')
print(f'{df_invoices.shape[0]} rows and {df_invoices.shape[1]} columns have been loaded into INVOICES in Snowflake.')

print('Check Snowflake!!')
