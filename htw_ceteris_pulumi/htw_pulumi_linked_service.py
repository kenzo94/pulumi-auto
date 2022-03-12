import pulumi
import pulumi_azure_native as azure_native

from pulumi import Output
#https://www.pulumi.com/docs/intro/concepts/inputs-outputs/#apply

def createLSSourceASQLandReturn(factory_name_auto,linked_service_name_sql,source_server_name_sql_auto,source_server_port_sql_auto,source_server_alias_sql_auto,userid,psw,resource_group_name_auto):
    
    connection_string_output = Output.all(source_server_name_sql_auto,source_server_alias_sql_auto) \
    .apply(lambda args:f"Server=tcp:{args[0]}.database.windows.net,{source_server_port_sql_auto};Initial Catalog={args[1]};Persist Security Info=False;User ID={userid};Password={psw};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;")

    factory_name_output = Output.all(factory_name_auto) \
    .apply(lambda args:args[0])
    
    resource_group_name_output = Output.all(resource_group_name_auto) \
    .apply(lambda args:args[0])


    linked_service_sql_db2 = azure_native.datafactory.LinkedService("linkedServiceDB",
        factory_name=factory_name_output,
        linked_service_name=linked_service_name_sql,
        properties=azure_native.datafactory.AzureSqlDatabaseLinkedServiceArgs(
            connection_string={
                "type": "SecureString",
                "value": connection_string_output,#"Server=tcp:"+source_server_name_sql_auto+".database.windows.net,"+source_server_port_sql_auto+";Initial Catalog="+source_server_alias_sql_auto +";Persist Security Info=False;User ID="+userid+";Password="+psw+";MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;",
            },
            type="AzureSqlDatabase",
        ),
        resource_group_name=resource_group_name_output)
    return linked_service_sql_db2

def createLSABLBandReturn(factory_name_auto,linked_service_name_blob,blob_account_name_auto, blob_account_key_auto,resource_group_name_auto):
    connection_string_output = Output.all(blob_account_name_auto,blob_account_key_auto) \
    .apply(lambda args:f"DefaultEndpointsProtocol=https;AccountName={args[0]};AccountKey={args[1]};EndpointSuffix=core.windows.net")

    factory_name_output = Output.all(factory_name_auto) \
    .apply(lambda args:args[0])
    
    resource_group_name_output = Output.all(resource_group_name_auto) \
    .apply(lambda args:args[0])

    linked_service_blob = azure_native.datafactory.LinkedService("linkedServiceBlob",
        factory_name=factory_name_output,
        linked_service_name=linked_service_name_blob,
        properties=azure_native.datafactory.AzureBlobStorageLinkedServiceArgs(
            account_kind="BlobStorage",
            connection_string={
                "type": "SecureString",
                "value": connection_string_output,#"DefaultEndpointsProtocol=https;AccountName="+blob_account_name_auto+";AccountKey="+blob_account_key_auto+";EndpointSuffix=core.windows.net",
            },
            type="AzureBlobStorage",
        ),
        resource_group_name=resource_group_name_output)
    return linked_service_blob
    
def createLSTargetADLSandReturn(factory_name_auto,linked_service_name_datalake,data_lake_account_name_auto, data_lake_account_key_auto,resource_group_name_auto  ):
    connection_string_output = Output.all(data_lake_account_name_auto) \
    .apply(lambda args:f"https://{args[0]}.dfs.core.windows.net/")


    factory_name_output = Output.all(factory_name_auto) \
    .apply(lambda args:args[0])
    
    resource_group_name_output = Output.all(resource_group_name_auto) \
    .apply(lambda args:args[0])

    linked_service_datalake = azure_native.datafactory.LinkedService("linkedServiceDatalake",
        factory_name=factory_name_output,
        linked_service_name=linked_service_name_datalake,
        properties=azure_native.datafactory.AzureBlobFSLinkedServiceArgs(
            url=connection_string_output,#"https://"+data_lake_account_name_auto+".dfs.core.windows.net/",
            account_key=data_lake_account_key_auto,
            type="AzureBlobFS",
        ),
        resource_group_name=resource_group_name_output)
    return linked_service_datalake
