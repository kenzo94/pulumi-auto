import pulumi
import pulumi_azure_native as azure_native

from pulumi import Output
#https://www.pulumi.com/docs/intro/concepts/inputs-outputs/#apply

def createLSSourceASQLandReturn(factory,linked_service_sql_name,server,source_server_port_sql_auto,database,userid,psw,resource_group):
    """
    :param factory obj: factory object for specific resource_group
    :param linked_service_sql_name str: new ame of linked service
    :param server obj: created server object
    :param source_server_port_sql_auto str: port
    :param database obj: created database object on server
    :param userid str: admin account name
    :param pwd str: password
    :param resource_group obj: resource group object
    
    :return linked_service object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/linkedservice/
    """
    
    connection_string_output = Output.all(server.name,database.name) \
    .apply(lambda args:f"Server=tcp:{args[0]}.database.windows.net,{source_server_port_sql_auto};Initial Catalog={args[1]};Persist Security Info=False;User ID={userid};Password={psw};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;")

    linked_service = azure_native.datafactory.LinkedService(linked_service_sql_name,
        factory_name=factory.name,
        linked_service_name=linked_service_sql_name,
        properties=azure_native.datafactory.AzureSqlDatabaseLinkedServiceArgs(
            connection_string={
                "type": "SecureString",
                "value": connection_string_output,#"Server=tcp:"+source_server_name_sql_auto+".database.windows.net,"+source_server_port_sql_auto+";Initial Catalog="+source_server_alias_sql_auto +";Persist Security Info=False;User ID="+userid+";Password="+psw+";MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;",
            },
            type="AzureSqlDatabase",
        ),
        resource_group_name=resource_group.name)
    return linked_service 

def createLSABLBandReturn(factory,linked_service_name_blob,blob_account, blob_account_key_auto,resource_group):
    """
    :param factory obj: factory object for specific resource_group
    :param linked_service_name_blob str: new name of linked service
    :param blob_account obj: created blob account object
    :param blob_account_key_auto obj: storage account access key object 
    :param resource_group obj: resource group object
    
    :return linked_service object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/linkedservice/
    """
    connection_string_output = Output.all(blob_account.name,blob_account_key_auto) \
    .apply(lambda args:f"DefaultEndpointsProtocol=https;AccountName={args[0]};AccountKey={args[1]};EndpointSuffix=core.windows.net")
    

    linked_service = azure_native.datafactory.LinkedService(linked_service_name_blob,
        factory_name=factory.name,
        linked_service_name=linked_service_name_blob,
        properties=azure_native.datafactory.AzureBlobStorageLinkedServiceArgs(
            account_kind="BlobStorage",
            connection_string={
                "type": "SecureString",
                "value": connection_string_output,#"DefaultEndpointsProtocol=https;AccountName="+blob_account_name_auto+";AccountKey="+blob_account_key_auto+";EndpointSuffix=core.windows.net",
            },
            type="AzureBlobStorage",
        ),
        resource_group_name=resource_group.name)
    return linked_service
    
def createLSTargetADLSandReturn(factory,linked_service_name_datalake,storage_account_target, data_lake_account_key_auto,resource_group):
    """
    :param factory obj: factory object for specific resource_group
    :param linked_service_name_datalake str: new name of linked service
    :param storage_account_target obj: created storage account object
    :param data_lake_account_key_auto obj: storage account access key object 
    :param resource_group obj: resource group object
    
    :return linked_service object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/linkedservice/
    """
    connection_string_output = Output.all(storage_account_target.name) \
    .apply(lambda args:f"https://{args[0]}.dfs.core.windows.net/")

    linked_service = azure_native.datafactory.LinkedService(linked_service_name_datalake,
        factory_name=factory.name,
        linked_service_name=linked_service_name_datalake,
        properties=azure_native.datafactory.AzureBlobFSLinkedServiceArgs(
            url=connection_string_output,#"https://"+data_lake_account_name_auto+".dfs.core.windows.net/",
            account_key=data_lake_account_key_auto,
            type="AzureBlobFS",
        ),
        resource_group_name=resource_group.name)
    return linked_service
