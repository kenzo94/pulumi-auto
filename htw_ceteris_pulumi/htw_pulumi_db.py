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

# sp for watermark and error
# CREATE PROCEDURE [dbo].[UpdateErrorTable]
# (
#     -- Add the parameters for the stored procedure here
#    @DataFactory_Name [nvarchar](500) NULL,
#    @Pipeline_Name [nvarchar](500) NULL,
#    @RunId [nvarchar](500) NULL,
#    @Source [nvarchar](500) NULL,
#    @Destination [nvarchar](500) NULL,
#    @Source_Type [nvarchar](500) NULL,
#    @Sink_Type [nvarchar](500) NULL,
#    @Execution_Status [nvarchar](500) NULL,
#    @ErrorDescription [nvarchar](max) NULL,
#    @ErrorCode [nvarchar](500) NULL,
#    @ErrorLoggedTime [nvarchar](500) NULL,
#    @FailureType [nvarchar](500) NULL
# )
# AS
# INSERT INTO [dbo].[azure_error_log]
# (
#    [DataFactory_Name],
#    [Pipeline_Name],
#    [RunId],
#    [Source],
#    [Destination],
#    [Source_Type],
#    [Sink_Type],
#    [Execution_Status],
#    [ErrorDescription],
#    [ErrorCode],
#    [ErrorLoggedTime],
#    [FailureType]
# )
# VALUES
# (
#    @DataFactory_Name,
#    @Pipeline_Name,
#    @RunId,
#    @Source,
#    @Destination,
#    @Source_Type,
#    @Sink_Type,
#    @Execution_Status,
#    @ErrorDescription,
#    @ErrorCode,
#    @ErrorLoggedTime,
#    @FailureType
# )
# GO


# SET ANSI_NULLS ON
# GO

# SET QUOTED_IDENTIFIER ON
# GO

# CREATE PROCEDURE [dbo].[usp_write_watermark] @modifiedDate datetime, @TableName varchar(50)
# AS

# BEGIN

# UPDATE watermarktable
# SET [WatermarkValue] = @modifiedDate
# WHERE [TableName] = @TableName

# END
# GO


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
                    WHERE TABLE_SCHEMA = 'dbo' 
                         AND  TABLE_NAME = 'DUMMY_DATA_TABLE_{i}'))
                       BEGIN
                           CREATE TABLE DUMMY_DATA_TABLE_{i}(
								TEST_NAME varchar(255) NOT NULL,
							);
                       END;""")
                #row= cursor.execute("""Select * FROM SalesLT.Product""").fetchone()
                #if row:print(row)