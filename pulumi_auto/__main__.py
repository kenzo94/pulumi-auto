"""An Azure RM Python Pulumi program"""

#main.py is the Pulumi program that defines our stack resources.

import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import storage
from pulumi_azure_native import resources
from htw_pulumi_linked_service import createLSSourceASQLandReturn
from htw_pulumi_linked_service import createLSABLBandReturn
from htw_pulumi_linked_service import createLSTargetADLSandReturn
from htw_pulumi_data_flow import createDataFlowAndReturn
from htw_pulumi_dataset import createDatasetABLBAndReturn
from htw_pulumi_dataset import createDatasetADLSAndReturn
from htw_pulumi_dataset import createDatasetASQLAndReturn
#Gedanken über Benamung
#import pulumi_azure as Azure

# Create an Azure Resource Group This Pulumi program creates an Azure resource group and storage account and then exports the storage account’s primary key.
resource_group = resources.ResourceGroup('resource_group',
 resource_group_name='pulumiauto')

# Create an Azure resource (Storage Account)
account_source = storage.StorageAccount('cethtwstorage',   
    account_name='plsource1',
    resource_group_name=resource_group.name,
    sku=storage.SkuArgs(
        name=storage.SkuName.STANDARD_LRS,
    ),
    kind=storage.Kind.STORAGE_V2)

    #BlobContainer
blob_container = azure_native.storage.BlobContainer("blobContainer",
    account_name=account_source.name,
    container_name="contpl1",
    default_encryption_scope="encryptionscope185", #notwendig?
    deny_encryption_scope_override=True,
    resource_group_name=resource_group.name)


account_dest = storage.StorageAccount('cethtwstorage3', 
    account_name='pldestination',
    resource_group_name=resource_group.name,
    sku=storage.SkuArgs(
        name=storage.SkuName.STANDARD_LRS,
    ),
    kind=storage.Kind.STORAGE_V2)



# Export the primary key of the Storage Account - kann später dynamisch übergeben werden für Linked Services für accout 2
primary_key = pulumi.Output.all(resource_group.name, account_source.name) \
    .apply(lambda args: storage.list_storage_account_keys(
        resource_group_name=args[0],
        account_name=args[1]
    )).apply(lambda accountKeys: accountKeys.keys[0].value)

pulumi.export("primary_storage_key", primary_key)

#Azure Datafactory
factory = azure_native.datafactory.Factory("factory",
    factory_name="htwcetdatafactory",
    resource_group_name=resource_group.name)



#SqlServer

server = azure_native.sql.Server("server",
    administrator_login="Team4Admin",
    administrator_login_password="OZh2fwL3TUqSzFO0fwfc", #später lösen, sodass sie nicht mehr im sourcecode sind
    resource_group_name=resource_group.name,
    server_name="htw-cet-sqlserver",
    public_network_access = "Enabled",
    minimal_tls_version="1.2")


#Database
database = azure_native.sql.Database("database",
    database_name="DWH",
    #kind="v12.0,user,vcore,serverless",
    resource_group_name=resource_group.name,
    server_name=server.name,
    sku=azure_native.sql.SkuArgs(
        capacity=6,
        family="Gen5",
        name="GP_S_Gen5",
        tier="GeneralPurpose"
    ),
    collation="SQL_Latin1_General_CP1_CI_AS",
    catalog_collation="SQL_Latin1_General_CP1_CI_AS",
    auto_pause_delay=60,
    min_capacity=1,
    requested_backup_storage_redundancy="Local"
    
    )

database = azure_native.sql.Database("dbsource1",
    database_name="DBSource1",
    #kind="v12.0,user,vcore,serverless",
    resource_group_name=resource_group.name,
    server_name=server.name,
    sku=azure_native.sql.SkuArgs(
        capacity=6,
        family="Gen5",
        name="GP_S_Gen5",
        tier="GeneralPurpose"
    ),
    collation="SQL_Latin1_General_CP1_CI_AS",
    auto_pause_delay=60,
    min_capacity=1,
    catalog_collation="SQL_Latin1_General_CP1_CI_AS",
    requested_backup_storage_redundancy="Local",
    sample_name="AdventureWorksLT"
    )

    #ConfigDatenbank Basic Tier 2-3 tabellen aufnehmen (Linked Service)

#Hanna: Pipeline erst ohne Parameter
#general info
factory_name_auto = "htwcetdatafactory"
resource_group_name_auto="pulumiauto"
psw="OZh2fwL3TUqSzFO0fwfc"
userid="Team4Admin"

#Source auto
source_server_name_sql_auto = server.name
source_server_port_sql_auto = "1433"
source_server_alias_sql_auto = database.name

# Blob auto
blob_account_name_auto = "plsource1"
blob_account_key_auto = "amu31xt5j91azbjZsACM5VaDMIIEnIj8Y3aYOKmX1aCbdLRCJxI/lLNpbno/X1nKHlPxyYfi3v3aSakEKB6Gpw=="

# Datalake auto
data_lake_account_name_auto = "pldestination"
data_lake_account_key_auto = "SOKULBYhq4QRX6XeVDKZTnoYffi9iPZeEmKqLArrt+5gPl+MP+46AOp3V6r/wBxq2jEK+xiZd87v1/XJILwk1A=="

#linked service names
linked_service_name_sql="LS_ASQL_SalesLT"
linked_service_name_blob ="LS_ABLB_CSV"
linked_service_name_datalake ="LS_ADLS_Target"

# create linked services
linked_service_sql_db2 = createLSSourceASQLandReturn(factory_name_auto,linked_service_name_sql,source_server_name_sql_auto,source_server_port_sql_auto,source_server_alias_sql_auto,userid,psw,resource_group_name_auto)
linked_service_blob =createLSABLBandReturn(factory_name_auto,linked_service_name_blob,blob_account_name_auto, blob_account_key_auto,resource_group_name_auto)
linked_service_datalake = createLSTargetADLSandReturn(factory_name_auto,linked_service_name_datalake,data_lake_account_name_auto, data_lake_account_key_auto,resource_group_name_auto)

# create datasets
dataset_asql = createDatasetASQLAndReturn("SalesLT",factory_name_auto,linked_service_sql_db2,resource_group_name_auto)
dataset_blob = createDatasetABLBAndReturn("Email",factory_name_auto,linked_service_blob,resource_group_name_auto)
dataset_dl_archiv = createDatasetADLSAndReturn("Archiv",factory_name_auto,linked_service_datalake,resource_group_name_auto)
dataset_dl_import = createDatasetADLSAndReturn("Import",factory_name_auto,linked_service_datalake,resource_group_name_auto)
dataset_dl_temp = createDatasetADLSAndReturn("Temp",factory_name_auto,linked_service_datalake,resource_group_name_auto)

# create dataflows
data_flow_csv_email = createDataFlowAndReturn("Email","Identifier",factory_name_auto,resource_group_name_auto,linked_service_datalake)
data_flow_csv_product = createDataFlowAndReturn("Product","ProductID",factory_name_auto,resource_group_name_auto,linked_service_datalake)
data_flow_csv_address = createDataFlowAndReturn("Address","AddressID",factory_name_auto,resource_group_name_auto,linked_service_datalake)




