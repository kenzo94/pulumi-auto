import pulumi
import pulumi_azure_native as azure_native

# create datasets
dataset_source_db_name = "DS_ASQL_DB"
dataset_blob_name = "DS_ABLB_Email"
dataset_data_lake_import_name  = "DS_ADLS_Import"
dataset_data_lake_archiv_name  = "DS_ADLS_Archiv"
dataset_data_lake_temp_name  = "DS_ADLS_Temp"

# db_name="SalesLT" create db dataset which contains all tables azure sql

def createDatasetASQLAndReturn(db_name,factory_name_auto,linked_service_sql,resource_group_name_auto):

    dataset = azure_native.datafactory.Dataset("dataset"+db_name,
        dataset_name= "DS_ASQL_"+ db_name,
        factory_name=factory_name_auto,
        properties=azure_native.datafactory.AzureSqlTableDatasetArgs(
            folder= {
                "name": db_name
            },
            linked_service_name=azure_native.datafactory.LinkedServiceReferenceArgs(
                reference_name= linked_service_sql.name,
                type= "LinkedServiceReference"
            ),
            type="AzureSqlTable",
        ),
        resource_group_name=resource_group_name_auto)
    return dataset

#create blob dataset for filename of type CSV; file_name="email.csv"; Container = "source"

def createDatasetABLBAndReturn(table_name,factory_name_auto,linked_service_blob,resource_group_name_auto):
    dataset = azure_native.datafactory.Dataset("dataset"+table_name.capitalize(),
        dataset_name="DS_ABLB_"+table_name,
        factory_name=factory_name_auto,
        properties=azure_native.datafactory.DelimitedTextDatasetArgs(
            folder= {
                "name": "CSV"
            },
            linked_service_name=azure_native.datafactory.LinkedServiceReferenceArgs(
                reference_name= linked_service_blob.name,
                type= "LinkedServiceReference"
            ),
            location = azure_native.datafactory.AzureBlobStorageLocationArgs(
                type ="AzureBlobStorageLocation",
                file_name = table_name.lower()+".csv",
                container = "source",
            ),
            column_delimiter = ";",
            escape_char= "\\",
            first_row_as_header = True,
            quote_char= "\"",
            
            type="DelimitedText",
        ),
        resource_group_name=resource_group_name_auto)
    return dataset

#folder_name = Import,Archiv,Temp
def createDatasetADLSAndReturn(folder_name,factory_name_auto,linked_service_datalake,resource_group_name_auto):

    dataset = azure_native.datafactory.Dataset("datasetDL"+folder_name.capitalize(),
        dataset_name="DS_ADLS_"+folder_name.capitalize(),
        factory_name=factory_name_auto,
        properties=azure_native.datafactory.ParquetDatasetArgs(
            folder= {
                "name": "DataLake"
            },
            linked_service_name=azure_native.datafactory.LinkedServiceReferenceArgs(
                reference_name= linked_service_datalake.name,
                type= "LinkedServiceReference"
            ),
            parameters= {
                "filename":{
                    "type": "string"
                }
            },
            location = azure_native.datafactory.AzureBlobFSLocationArgs(
                type ="AzureBlobFSLocation",
                file_name = {
                        "value": "@dataset().filename",
                        "type": "Expression"
                    },
                file_system = folder_name.lower()
            ),
            compression_codec= "snappy",
            type="Parquet",
        ),
        resource_group_name=resource_group_name_auto)
    return dataset