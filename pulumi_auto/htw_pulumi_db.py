#Mac installastion
#brew install unixodbc
#pip install pyodbc
#https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15
# watermark + metadaten (Join Tables)
#https://www.microsoft.com/en-us/sql-server/developer-get-started/python/mac/step/2.html
import pyodbc


name_of_meta_table ="HTW_META_DATA_TABLE"
name_of_schema ='dbo'
meta_table =[]
# Add MetaRow Into MetaTable
def addMetaRowToMetaTable(meta_row):
    meta_table.append(meta_row)
# Create MetaRow
def createMetaRow(table_name,table_schema,key_column,table_type):
    return dict({'table_name': table_name,
                 'table_schema':table_schema,
                'key_column': key_column,
                'table_type':table_type})
# Return Meta Table
def getMetaTable():
    server = 'htw-cet-sqlserver.database.windows.net'#'htw-cet-sqlserver.database.windows.net' # replace server name with variable
    database = 'DBSource1'#'DBSource1' # replace with variable
    username = 'Team4Admin' # replace with variable of class
    password = 'OZh2fwL3TUqSzFO0fwfc' # replace with variable of class
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""IF (EXISTS (SELECT * 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'dbo' 
                    AND  TABLE_NAME = 'HTW_META_DATA_TABLE'))
                        BEGIN
                            drop table [dbo].[HTW_META_DATA_TABLE]
                        END;""")
            cursor.execute("""
                    SELECT 
                        kcu.TABLE_NAME,
                        kcu.TABLE_SCHEMA,
                        (SELECT Top 1 k.COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k 
                            WHERE kcu.TABLE_NAME = k.TABLE_NAME
                            AND kcu.TABLE_SCHEMA = k.TABLE_SCHEMA
                            AND k.COLUMN_NAME like '%id%') AS 'TABLE_ID',
                        'SQL' as 'TABLE_TYPE'
                        INTO [dbo].[HTW_META_DATA_TABLE]
                        FROM INFORMATION_SCHEMA.TABLES kcu
                        WHERE kcu.TABLE_TYPE = 'BASE TABLE'
                        AND kcu.TABLE_SCHEMA ='SalesLT'""")
            # Insert CSV Data
            cursor.execute("""
                    INSERT INTO """+name_of_meta_table+""" (TABLE_NAME, TABLE_SCHEMA, TABLE_ID, TABLE_TYPE)
                    VALUES ('Email', 'Manual' , 'Identifier', 'CSV');""")

            cursor.execute("""
                    SELECT * FROM HTW_META_DATA_TABLE""")            
            row = cursor.fetchone()
            while row:
                print(row)
                addMetaRowToMetaTable(createMetaRow(row[0],row[1],row[2],row[3]))
                row = cursor.fetchone()
            print(meta_table)
    return meta_table
