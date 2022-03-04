#Mac installastion
#brew install unixodbc
#pip install pyodbc
#https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15
# watermark + metadaten (Join Tables)
#https://www.microsoft.com/en-us/sql-server/developer-get-started/python/mac/step/2.html
import pyodbc

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
    
def sql_connect():
    pass

# Return Meta Table
def getMetaTable():
    server = 'htw-cet-sqlserver.database.windows.net'#'htw-cet-sqlserver.database.windows.net' # replace server name with variable
    database = 'DBSource1'#'DBSource1' # replace with variable
    username = 'Team4Admin' # replace with variable of class
    password = 'OZh2fwL3TUqSzFO0fwfc' # replace with variable of class
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
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
                    ('Email', 'Manual' , 'Identifier', 'CSV')

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

            cursor.execute("""
                    SELECT * FROM [dbo].[HTW_META_DATA_TABLE]
                    """)            
            row = cursor.fetchone()
            while row:
                #print(row)
                addMetaRowToMetaTable(createMetaRow(row[0],row[1],row[2],row[3]))
                row = cursor.fetchone()
            print(meta_table)
    return meta_table

def createsample():
    server = 'htw-cet-sqlserver.database.windows.net'#'htw-cet-sqlserver.database.windows.net' # replace server name with variable
    database = 'DBSource1'#'DBSource1' # replace with variable
    username = 'Team4Admin' # replace with variable of class
    password = 'OZh2fwL3TUqSzFO0fwfc' # replace with variable of class
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        with conn.cursor() as cursor:
        #forschleife + Namensänderung 
            for i in range(1, 1000):
                cursor.execute(f"""IF (NOT EXISTS (SELECT * 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'SalesLT' 
                         AND  TABLE_NAME = 'DUMMY_DATA_TABLE_{i}'))
                       BEGIN
                           CREATE TABLE [SalesLT].[DUMMY_DATA_TABLE_{i}](
								DUMMY_DATA varchar(255),
                                ModifiedDate datetime
							)
                            INSERT INTO [SalesLT].[DUMMY_DATA_TABLE_{i}]
                            VALUES('dummy', '1/1/2022');
                       END;""")
                #row= cursor.execute("""Select * FROM SalesLT.Product""").fetchone()
                #if row:print(row)

def create_stored_procedure():
    server = 'htw-cet-sqlserver.database.windows.net'#'htw-cet-sqlserver.database.windows.net' # replace server name with variable
    database = 'DBSource1'#'DBSource1' # replace with variable
    username = 'Team4Admin' # replace with variable of class
    password = 'OZh2fwL3TUqSzFO0fwfc' # replace with variable of class
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
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
              
            cursor.execute("""IF (NOT EXISTS (SELECT * 
                    FROM SYS.OBJECTS 
                    WHERE TYPE = 'P' 
                    AND  OBJECT_ID = OBJECT_ID('dbo.usp_write_watermark')))
                        exec('CREATE PROCEDURE [dbo].[usp_write_watermark] AS BEGIN SET NOCOUNT ON; END')
                        GO
                        
                        ALTER PROCEDURE [dbo].[usp_write_watermark] @modifiedDate datetime, @TableName varchar(50)
                        AS
                        BEGIN

                        UPDATE watermarktable
                        SET [WatermarkValue] = @modifiedDate
                        WHERE [TableName] = @TableName

                        END
                        """)
            
            cursor.execute("""IF (NOT EXISTS (SELECT * 
                    FROM SYS.OBJECTS 
                    WHERE TYPE = 'P' 
                    AND  OBJECT_ID = OBJECT_ID('dbo.usp_update_error_table')))
                        exec('CREATE PROCEDURE [dbo].[usp_update_error_table] AS BEGIN SET NOCOUNT ON; END')
                        GO
                        
                        ALTER PROCEDURE [dbo].[usp_update_error_table]
                        (
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
                        END
                        """)

def fill_watermark_table():
    server = 'htw-cet-sqlserver.database.windows.net'#'htw-cet-sqlserver.database.windows.net' # replace server name with variable
    database = 'DBSource1'#'DBSource1' # replace with variable
    username = 'Team4Admin' # replace with variable of class
    password = 'OZh2fwL3TUqSzFO0fwfc' # replace with variable of class
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
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
                    IF NOT EXISTS(SELECT 1 FROM [dbo].[usp_write_watermark] WHERE TableName={row.TABLE_NAME})
                        INSERT INTO [dbo].[usp_write_watermark]
                        VALUES({row.TABLE_NAME},'1//1/2000');
                    """) 