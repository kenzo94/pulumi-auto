import pulumi
import pulumi_azure_native as azure_native


def createDatasetASQLAndReturn(db_name,factory,linked_service_sql,resource_group):
    """
    :param db_name str: name of asql database used for creation of name and folder
    :param factory obj: factory object for specific resource_group
    :param linked_service_sql obj: linked service created for ASQL Database for specific resource_group
    :param resource_group obj: resource group object
    
    :return dataset object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/dataset/
    """

    dataset = azure_native.datafactory.Dataset("dataset"+db_name,
        dataset_name= "DS_ASQL_"+ db_name,
        factory_name=factory.name,
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
        resource_group_name=resource_group.name)
    return dataset

#create blob dataset for filename of type CSV; file_name="email.csv"; Container = "source"

def createDatasetABLBAndReturn(table_name,dataset_folder_name,blob_storage_container_name,factory,linked_service_blob,resource_group):
    """
    :param table_name str: will be used to create Database name (avoid special chars in name) and to concat fiie name and .csv extension
    :param dataset_folder_name str: name of folder, where newly created dataset will be stored
    :param blob_storage_container_name str: name of source folder, where csv files will be stored
    :param factory obj: factory object for specific resource_group
    :param linked_service_blob obj: linked service created for Azure Blob Storage for specific resource_group
    :param resource_group obj: resource group object
    
    :return dataset object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/dataset/
    """
    dataset = azure_native.datafactory.Dataset("dataset"+table_name.capitalize(),
        dataset_name="DS_ABLB_"+table_name,
        factory_name=factory.name,
        properties=azure_native.datafactory.DelimitedTextDatasetArgs(
            folder= {
                "name": dataset_folder_name
            },
            linked_service_name=azure_native.datafactory.LinkedServiceReferenceArgs(
                reference_name= linked_service_blob.name,
                type= "LinkedServiceReference"
            ),
            location = azure_native.datafactory.AzureBlobStorageLocationArgs(
                type ="AzureBlobStorageLocation",
                file_name = table_name.lower()+".csv",
                container = blob_storage_container_name,
            ),
            column_delimiter = ";",
            escape_char= "\\",
            first_row_as_header = True,
            quote_char= "\"",
            
            type="DelimitedText",
        ),
        resource_group_name=resource_group.name)
    return dataset

#folder_name = Import,Archiv,Temp
def createDatasetADLSAndReturn(folder_name,folder_name_to_store_dataset,factory,linked_service_datalake,resource_group):
    """
    :param folder_name str: name of the folder in datalake container in this project ("Import","Temp","Archiv")
    :param folder_name_to_store_dataset str: name of the folder, where newly created dataset will be stored
    :param factory obj: factory object for specific resource_group
    :param linked_service_datalake obj: linked service created for Datalake for specific resource_group
    :param resource_group obj: resource group object
    
    :return dataset object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/dataset/
    """

    dataset = azure_native.datafactory.Dataset("datasetDL"+folder_name.capitalize(),
        dataset_name="DS_ADLS_"+folder_name.capitalize(),
        factory_name=factory.name,
        properties=azure_native.datafactory.ParquetDatasetArgs(
            folder= {
                "name": folder_name_to_store_dataset #"DataLake"
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
        resource_group_name=resource_group.name)
    return dataset