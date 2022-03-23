#https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15
#https://www.microsoft.com/en-us/sql-server/developer-get-started/python/mac/step/2.html

import pyodbc
## Initialize empty list
meta_table_main =[]
## Used for Tests
#meta_table_10= [{'table_name': 'Email', 'table_schema': 'Manual', 'key_column': 'Identifier', 'table_type': 'CSV'}, {'table_name': 'Emailzwei', 'table_schema': 'Manual', 'key_column': 'Identifier', 'table_type': 'CSV'}, {'table_name': 'Product', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductCategory', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductDescription', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductModel', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'ProductModelProductDescription', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'SalesOrderDetail', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}, {'table_name': 'SalesOrderHeader', 'table_schema': 'SalesLT', 'key_column': 'rowguid', 'table_type': 'SQL'}]

   
## Add MetaRow Into MetaTable
def addMetaRowToMetaTable(meta_row):
    meta_table_main.append(meta_row)
    
## Create MetaRow
def createMetaRow(table_name,table_schema,key_column,table_type):
    return dict({'table_name': table_name,
                 'table_schema':table_schema,
                'key_column': key_column,
                'table_type':table_type})

## Establisch Connection to Source Database and return Connection Object    
def establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+serverName+'.database.windows.net;PORT=1433;DATABASE='+dbSourceName+';UID='+dbSourceUserName+';PWD='+ dbSourcePSW)
    return conn

## FILL META TABLE ('HTW_META_DATA_TABLE')
### SQL Tables
#### Results are filtered on TABLE_SCHEMA in('SalesLT');
#### TABLE_ID is Top1 of SELECT Results and COLUMN_NAME like '%id%'
#### INFORMATION_SCHEMA.TABLES is a source Table for Creation od META TABLE
#### TABLE_NAME schould be unique in this HTW_META_DATA_TABLE
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
            
### CSV Files
#### List can be extended by adding new Values pair after ('Email', 'Manual' , 'Identifier', 'CSV'),
#### Name of CSV TABLE_NAME schould be the same as File Name and schould not contain special charecters like "_" (Dataflow Name does not accept them)
#### TABLE_NAME schould be unique in this HTW_META_DATA_TABLE
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
                    ('Emailzwei', 'Manual' , 'Identifier', 'CSV')

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

## RETURN META TABLE RESULTS AS LIST OF DICTs
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

## CREATE AND FILL DUMMY TABLES FOR MASS TESTING
### As we use the AdventureWorksLT in our Database definition, we can use the schema "SalesLT"
### We fill the table with some dummy data and a ModifiedDate to check for delta load
def create_sample(serverName,dbSourceName,dbSourceUserName,dbSourcePSW):
    with establishDBConnection(serverName,dbSourceName,dbSourceUserName,dbSourcePSW) as conn:
        with conn.cursor() as cursor:
        #for loop to create 100 sample table
            for i in range(1, 100):
                cursor.execute(f"""IF (NOT EXISTS (SELECT * 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'SalesLT' 
                         AND  TABLE_NAME = 'DummyTable{i}'))
                       BEGIN
                           CREATE TABLE [SalesLT].[DummyTable{i}](
                                ID int not null identity primary key,
								DUMMY_DATA varchar(255),
                                ModifiedDate datetime
							)
                            INSERT INTO [SalesLT].[DummyTable{i}]
                            VALUES('dummy', '1/1/2022');
                       END;""")
                

## CREATE SYSTEM TABLES (Watermark & Error Logs)
### Watertable is use for our delta load logic and error is use for our error logging
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
              
 ## CREATE WATERMARK STORED PROCEDURE
 ### This sp is use to fill the watermark table created ealier
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
            
            
 ## CREATE ERROR LOG STORED PROCEDURE
 ### This spi is use to fill the error log table created ealier
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
                       
                       
## FILL WATERMARK TABLE
### This query is use to fill the watermark table with init values to compare to at the first time everything is getting loaded                        
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
