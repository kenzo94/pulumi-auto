#Mac installastion
#brew install unixodbc
#pip install pyodbc
#https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15
# watermark + metadaten (Join Tables)
#https://www.microsoft.com/en-us/sql-server/developer-get-started/python/mac/step/2.html
import asyncio
import htw_config as cfg
import pyodbc
import time
from pulumi import Output

meta_table_main =[]
meta_table_100 =[{'table_name': 'Address', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'},{'table_name': 'Customer', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'CustomerAddress', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_1', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_10', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_11', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_12', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_13', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_14', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_15', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_16', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_17', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_18', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_19', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_2', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_20', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_21', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_22', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_23', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_24', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_25', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_26', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_27', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_28', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_29', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_3', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_30', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_31', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_32', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_33', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_34', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_35', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_36', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_37', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_38', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_39', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_4', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_40', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_41', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_42', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_43', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_44', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_45', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_46', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_47', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_48', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_49', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_5', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_50', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_51', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_52', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_53', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_54', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_55', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_56', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_57', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_58', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_59', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_6', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_60', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_61', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_62', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_63', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_64', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_65', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_66', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_67', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_68', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_69', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_7', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_70', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_71', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_72', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_73', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_74', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_75', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_76', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_77', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_78', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_79', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_8', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_80', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_81', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_82', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_83', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_84', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_85', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_86', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_87', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_88', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_89', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_9', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_90', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_91', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_92', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_93', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_94', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_95', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_96', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_97', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_98', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'DUMMY_DATA_TABLE_99', 'table_schema': 'SalesLT', 'key_column': 'ID', 'table_type': 'SQL'}, {'table_name': 'Email', 'table_schema': 'Manual', 'key_column': 'Identifier', 'table_type': 'CSV'}, {'table_name': 'Email_zwei', 'table_schema': 'Manual', 'key_column': 'Identifier', 'table_type': 'CSV'}, {'table_name': 'Product', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductCategory', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductDescription', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductModel', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductModelProductDescription', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'SalesOrderDetail', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'SalesOrderHeader', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}]
meta_table_10= [{'table_name': 'Email', 'table_schema': 'Manual', 'key_column': 'Identifier', 'table_type': 'CSV'}, {'table_name': 'Email_zwei', 'table_schema': 'Manual', 'key_column': 'Identifier', 'table_type': 'CSV'}, {'table_name': 'Product', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductCategory', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductDescription', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductModel', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductModelProductDescription', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'SalesOrderDetail', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'SalesOrderHeader', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}]

   
# Add MetaRow Into MetaTable
def addMetaRowToMetaTable(meta_row):
    meta_table_main.append(meta_row)
    
# Create MetaRow
def createMetaRow(table_name,table_schema,key_column,table_type):
    return dict({'table_name': table_name,
                 'table_schema':table_schema,
                'key_column': key_column,
                'table_type':table_type})
    
def establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+serverName+'.database.windows.net;PORT=1433;DATABASE='+dbSourceName+';UID='+dbSourceUserName+';PWD='+ dbSourcePSW)
    return conn

# Fill Meta Table
def fill_meta_table(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    with establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""IF (NOT EXISTS (SELECT * 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'dbo' 
                    AND  TABLE_NAME = 'HTW_META_DATA_TABLE'))
                        BEGIN
                            CREATE TABLE HTW_META_DATA_TABLE(
								TABLE_NAME varchar(255) NOT NULL,
								TABLE_SCHEMA varchar(255) NOT NULL,
								TABLE_ID varchar(255) NOT NULL,
								TABLE_TYPE varchar(255) NOT NULL,
								PRIMARY KEY (TABLE_NAME)
							);
                        END;""")
            cursor.execute("""
                CREATE TABLE #HTW_META_DATA_TABLE
                    (
                        TABLE_NAME varchar(255) NOT NULL,
                        TABLE_SCHEMA varchar(255) NOT NULL,
                        TABLE_ID varchar(255) NOT NULL,
                        TABLE_TYPE varchar(255) NOT NULL,
                        PRIMARY KEY (TABLE_NAME)
                    )
					
					INSERT INTO  #HTW_META_DATA_TABLE (TABLE_NAME, TABLE_SCHEMA, TABLE_ID, TABLE_TYPE)
                    SELECT
                    kcu.TABLE_NAME,
                    kcu.TABLE_SCHEMA,
                    (SELECT Top 1 k.COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k 
                                                WHERE kcu.TABLE_NAME = k.TABLE_NAME
                                                AND kcu.TABLE_SCHEMA = k.TABLE_SCHEMA
                                                AND k.COLUMN_NAME like '%id%') AS 'TABLE_ID',
                    'SQL' as 'TABLE_TYPE'
                    FROM INFORMATION_SCHEMA.TABLES kcu
                    WHERE kcu.TABLE_TYPE = 'BASE TABLE'
                    AND kcu.TABLE_SCHEMA in('SalesLT');
					
					MERGE [dbo].[HTW_META_DATA_TABLE] dst
                    USING #HTW_META_DATA_TABLE src
                    ON (dst.TABLE_NAME = src.TABLE_NAME)
                    WHEN MATCHED THEN
                    UPDATE SET
                    dst.TABLE_SCHEMA=src.TABLE_SCHEMA,
                    dst.TABLE_ID=src.TABLE_ID,
                    dst.TABLE_TYPE= src.TABLE_TYPE
                    WHEN NOT MATCHED THEN
                    INSERT VALUES (src.TABLE_NAME,src.TABLE_SCHEMA,src.TABLE_ID,src.TABLE_TYPE)
                    WHEN NOT MATCHED BY SOURCE AND dst.TABLE_TYPE='SQL' THEN
                    DELETE;

					SELECT * FROM [dbo].[HTW_META_DATA_TABLE];

                    DROP TABLE #HTW_META_DATA_TABLE;""")
            
            # Insert CSV Data, you can extend the list by adding comma after first insert set
            cursor.execute("""
                    CREATE TABLE #HTW_META_DATA_TABLE
                    (
                        TABLE_NAME varchar(255) NOT NULL,
                        TABLE_SCHEMA varchar(255) NOT NULL,
                        TABLE_ID varchar(255) NOT NULL,
                        TABLE_TYPE varchar(255) NOT NULL,
                        PRIMARY KEY (TABLE_NAME)
                    )
                    INSERT INTO #HTW_META_DATA_TABLE VALUES
                    ('Email', 'Manual' , 'Identifier', 'CSV'),
                    ('Email_zwei', 'Manual' , 'Identifier', 'CSV')

                    MERGE [dbo].[HTW_META_DATA_TABLE] dst
                    USING #HTW_META_DATA_TABLE src
                    ON (dst.TABLE_NAME = src.TABLE_NAME)
                    WHEN MATCHED THEN
                    UPDATE SET
                    dst.TABLE_SCHEMA=src.TABLE_SCHEMA,
                    dst.TABLE_ID=src.TABLE_ID,
                    dst.TABLE_TYPE= src.TABLE_TYPE
                    WHEN NOT MATCHED THEN
                    INSERT VALUES (src.TABLE_NAME,src.TABLE_SCHEMA,src.TABLE_ID,src.TABLE_TYPE)
                    WHEN NOT MATCHED BY SOURCE AND dst.TABLE_TYPE='CSV' THEN
                    DELETE;

                    DROP TABLE #HTW_META_DATA_TABLE;
                  """)

# Return Meta Table
def get_meta_table(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    try:
        conn = establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                        SELECT * FROM [dbo].[HTW_META_DATA_TABLE]
                        """)            
                row = cursor.fetchone()
                while row:
                    #print(row)
                    addMetaRowToMetaTable(createMetaRow(row[0],row[1],row[2],row[3]))
                    row = cursor.fetchone()
        return meta_table_main
    except:
        return []

def create_sample(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    with establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW) as conn:
        with conn.cursor() as cursor:
        #forschleife + Namensänderung 
            for i in range(1, 100):
                cursor.execute(f"""IF (NOT EXISTS (SELECT * 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'SalesLT' 
                         AND  TABLE_NAME = 'DUMMY_DATA_TABLE_{i}'))
                       BEGIN
                           CREATE TABLE [SalesLT].[DUMMY_DATA_TABLE_{i}](
                                ID int not null identity primary key,
								DUMMY_DATA varchar(255),
                                ModifiedDate datetime
							)
                            INSERT INTO [SalesLT].[DUMMY_DATA_TABLE_{i}]
                            VALUES('dummy', '1/1/2022');
                       END;""")
                #row= cursor.execute("""Select * FROM SalesLT.Product""").fetchone()
                #if row:print(row)

def create_system_tables(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    with establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""IF (NOT EXISTS (SELECT * 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'dbo' 
                         AND  TABLE_NAME = 'watermarktable'))
                       BEGIN
                           CREATE TABLE watermarktable(
								TableName varchar(255),
                                WatermarkValue datetime
							);
                       END;""")
             
            cursor.execute("""IF (NOT EXISTS (SELECT * 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'dbo' 
                         AND  TABLE_NAME = 'azure_error_log'))
                       BEGIN
                           CREATE TABLE azure_error_log(
                                DataFactory_Name [nvarchar](500),
                                Pipeline_Name [nvarchar](500),
                                RunId [nvarchar](500),
                                Source [nvarchar](500),
                                Destination [nvarchar](500),
                                Source_Type [nvarchar](500),
                                Sink_Type [nvarchar](500),
                                Execution_Status [nvarchar](500),
                                ErrorDescription [nvarchar](max),
                                ErrorCode [nvarchar](500),
                                ErrorLoggedTime [nvarchar](500),
                                FailureType [nvarchar](500)
							);
                       END;""")
              
 
def create_stored_procedure_watermark(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    with establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""IF NOT EXISTS (SELECT * FROM SYS.OBJECTS WHERE TYPE = 'P' AND  OBJECT_ID = OBJECT_ID('usp_write_watermark'))
                        exec('CREATE PROCEDURE [dbo].[usp_write_watermark] @modifiedDate datetime, @TableName varchar(255)
                        AS 
                        BEGIN
                        UPDATE watermarktable
                        SET [WatermarkValue] = @modifiedDate
                        WHERE [TableName] = @TableName
                        END;')""")
            cursor.commit()

def create_stored_procedure_error_log(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    with establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW) as conn:
        with conn.cursor() as cursor:
                       cursor.execute("""IF NOT EXISTS (SELECT * FROM SYS.OBJECTS WHERE TYPE = 'P' AND  OBJECT_ID = OBJECT_ID('usp_update_error_table'))
                        exec('CREATE PROCEDURE [dbo].[usp_update_error_table] (
                            @DataFactory_Name [nvarchar](500) NULL,
                            @Pipeline_Name [nvarchar](500) NULL,
                            @RunId [nvarchar](500) NULL,
                            @Source [nvarchar](500) NULL,
                            @Destination [nvarchar](500) NULL,
                            @Source_Type [nvarchar](500) NULL,
                            @Sink_Type [nvarchar](500) NULL,
                            @Execution_Status [nvarchar](500) NULL,
                            @ErrorDescription [nvarchar](max) NULL,
                            @ErrorCode [nvarchar](500) NULL,
                            @ErrorLoggedTime [nvarchar](500) NULL,
                            @FailureType [nvarchar](500) NULL
                         )
						AS 
                        BEGIN
                        INSERT INTO [dbo].[azure_error_log]
                         (
                            [DataFactory_Name],
                            [Pipeline_Name],
                            [RunId],
                            [Source],
                            [Destination],
                            [Source_Type],
                            [Sink_Type],
                            [Execution_Status],
                            [ErrorDescription],
                            [ErrorCode],
                            [ErrorLoggedTime],
                            [FailureType]
                         )
                         VALUES
                         (
                            @DataFactory_Name,
                            @Pipeline_Name,
                            @RunId,
                            @Source,
                            @Destination,
                            @Source_Type,
                            @Sink_Type,
                            @Execution_Status,
                            @ErrorDescription,
                            @ErrorCode,
                            @ErrorLoggedTime,
                            @FailureType
                         )
                        END;')""")
                       cursor.commit()
                        
def fill_watermark_table(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    with establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE='BASE TABLE';
                    """)
            rows = cursor.fetchall()            
            for row in rows:
                #print(row)
                cursor.execute(f"""
                    IF NOT EXISTS(SELECT 1 FROM [dbo].[watermarktable] WHERE TableName='{row.TABLE_NAME}')
                        INSERT INTO [dbo].[watermarktable]
                        VALUES('{row.TABLE_NAME}','1/1/2000');
                    """)

# async def test():
#     return True


# async def mainl():

#     # test() returns coroutine:
#     coro = test()

#     # we can await for coroutine to get result:
#     res = await coro    
#     print(res)  # True
# asyncio.create_task(mainl())