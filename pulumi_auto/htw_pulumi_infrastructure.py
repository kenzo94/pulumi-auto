#htw_pulumi_infrastructure.py defices the pulumi inftastructure 

from http import server
import re
import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import storage
from pulumi_azure_native import resources

def getpassword():
    return "OZh2fwL3TUqSzFO0fwfc"

def getuserid():
    return "Team4Admin"

#Infratsurcture als Methode

# Create an Azure Resource Group This Pulumi program creates an Azure resource group and storage account and then exports the storage account’s primary key.
def createResourceGroup(resource_group_name):
    resource_group = resources.ResourceGroup('resource_group_'+resource_group_name,
        resource_group_name=resource_group_name,
        location='WestEurope')

# Create an Azure resource (Storage Account)

def createAccoutSource(account_source_name, resource_group_name):
    account_source = storage.StorageAccount('account_source_'+account_source_name,   
        account_name=account_source_name,
        resource_group_name=resource_group_name,
        location='WestEurope',
        sku=storage.SkuArgs(
            name=storage.SkuName.STANDARD_LRS,
        ),
        kind=storage.Kind.STORAGE_V2)

def createBlobContainer(resource_group_name, account_source_name, blob_container_name):
    blob_container_name = azure_native.storage.BlobContainer('blob_container_' +blob_container_name,
        container_name=blob_container_name,
        resource_group_name=resource_group_name,
        account_name=account_source_name,
        deny_encryption_scope_override=True)

def createAccountDestination(account_destination_name,resource_group_name):
    account_destination= storage.StorageAccount('account_destination_' +account_destination_name,
        account_name=account_destination_name,
        resource_group_name=resource_group_name,
        location='WestEurope',
        sku=storage.SkuArgs(
            name=storage.SkuName.STANDARD_LRS,
        ),
        kind=storage.Kind.STORAGE_V2)

def createDataFactory(data_factory_name,resource_group_name):
    data_factory = azure_native.datafactory.Factory('data_factory_' +data_factory_name,
        factory_name= data_factory_name,
        resource_group_name=resource_group_name)

def createServer(server_name, resource_group_name):
    server = azure_native.sql.Server('server_' +server_name,
        server_name=server_name,
        administrator_login=getuserid(),
        administrator_login_password=getpassword(),
        resource_group_name=resource_group_name,
        location='WestEurope',
        public_network_access = "Enabled",
        minimal_tls_version="1.2")


# #Firewall aktivieren

def createFirewallRule(resource_group_name, server_name, firewall_rule_name):
    firewall_rule = azure_native.sql.FirewallRule('firewall_rule_' +firewall_rule_name,
        end_ip_address="255.255.255.255",
        resource_group_name=resource_group_name,
        server_name=server_name,
        start_ip_address="0.0.0.0")



# #Database
def createDatabase(resource_group_name, server_name, database_name,):
    database=azure_native.sql.Database('database_'+database_name,
    resource_group_name=resource_group_name,
    server_name=server_name,
    location='WestEurope',
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
     requested_backup_storage_redundancy="Local" )

#Create Database Source
def createDatabaseSource(resource_group_name,server_name,database_source_name, sample_name):
    database_source = azure_native.sql.Database('database_source_' +database_source_name,
        resource_group_name=resource_group_name,
        server_name=server_name,
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
        sample_name=sample_name) #"AdventureWorksLT" 





#resource_group_name = azure_native.resources.get_resource_group(resource_group_name=resource_group_name)
#data_factory= azure_native.datafactory.get_factory(data_factory=data_factory_name,resource_group_name=resource_group_name)
# print(factory.name)

def getResourceGroupName(resource_group_name):
    resource_group=resources.ResourceGroup(resource_group_name=resource_group_name)
    return resource_group.name

def getFactoryName(resource_group_name, factory_name):
     factory= azure_native.datafactory.get_factory(factory_name=factory_name,resource_group_name=resource_group_name)
     return factory.name

def getServername(server_name,resource_group_name):
    server=azure_native.sql.get_server(resource_group_name=resource_group_name, server_name=server_name)
    return server_name.name

def getAccountSource(account_source_name, resource_group_name):
    account_source=storage.get_storage_account(account_source=account_source_name, resource_group_name=resource_group_name)
    return account_source.name

def getAccountDestination(account_destination_name, resource_group_name):
    account_destination=storage.get_storage_account(account_name=account_destination_name, resource_group_name=resource_group_name)
    return account_destination.name

def getDatabaseSourceName(server_name, resource_group_name, database_source_name):
    database_source=azure_native.sql.get_database(database_source=database_source_name, resource_group_name=resource_group_name, server_name=server_name)
    return database_source.name


def getBlobAccountKeys(resource_group_name, account_source_name, blob_account_keys):
    blob_account_keys=storage.list_storage_account_keys(account_name=account_source_name, resource_group_name=resource_group_name)
    return blob_account_keys.keys[0]['value']

def getDataLakeKeys(account_destination_name, resource_group_name, data_lake_keys):
    data_lake_keys=storage.list_storage_account_keys(account_name=account_destination_name, resource_group_name=resource_group_name)
    return data_lake_keys.keys[0]['value']


#Alt
# server= azure_native.sql.get_server(resource_group_name=resource_group.name,server_name="htw-cet-sqlserver")
# print(server.name)
# database_dwh=azure_native.sql.get_database(database_name="DWH", resource_group_name=resource_group.name,server_name=server.name)#,"","htw-cet-sqlserver")
# print(database_dwh.name)
# database_source=azure_native.sql.get_database(database_name="DBSource1", resource_group_name=resource_group.name,server_name=server.name)
# print(database_source.name)
# account_source = storage.get_storage_account(account_name='plsource1',resource_group_name=resource_group.name)
# print(blob_account_keys.keys[0]['value'])
# account_dest  = storage.get_storage_account(account_name='plsource1',resource_group_name=resource_group.name)
# data_lake_keys = storage.list_storage_account_keys(account_name='pldestination',resource_group_name=resource_group.name)
# data_lake_account_key= data_lake_keys.keys[0]['value']
# print(data_lake_account_key)
#blob_account_keys = storage.list_storage_account_keys(account_name=getAccountSource,resource_group_name=resource_group_name)
#blob_account_key= blob_account_keys.keys[0]['value']

#create Methoden ab hier

#     #BlobContainer
# blob_container = azure_native.storage.BlobContainer("blobContainer",
#     account_name=account_source.name,
#     container_name="contpl1",
#     default_encryption_scope="encryptionscope185", #notwendig?
#     deny_encryption_scope_override=True,
#     resource_group_name=resource_group.name)


# account_dest = storage.StorageAccount('cethtwstorage3', 
#     account_name='pldestination',
#     resource_group_name=resource_group.name,
#     sku=storage.SkuArgs(
#         name=storage.SkuName.STANDARD_LRS,
#     ),
#     kind=storage.Kind.STORAGE_V2)



# # Export the primary key of the Storage Account - kann später dynamisch übergeben werden für Linked Services für accout 2
# primary_key = pulumi.Output.all(resource_group.name, account_source.name) \
#     .apply(lambda args: storage.list_storage_account_keys(
#         resource_group_name=args[0],
#         account_name=args[1]
#     )).apply(lambda accountKeys: accountKeys.keys[0].value)

# pulumi.export("primary_storage_key", primary_key)

# #Azure Datafactory
# factory = azure_native.datafactory.Factory("factory",
#     factory_name="htwcetdatafactory",
#     resource_group_name=resource_group.name)



# #SqlServer

# server = azure_native.sql.Server("server",
#     administrator_login=getuserid(),
#     administrator_login_password=getpassword(), #später lösen, sodass sie nicht mehr im sourcecode sind
#     resource_group_name=resource_group.name,
#     server_name="htw-cet-sqlserver",
#     public_network_access = "Enabled",
#     minimal_tls_version="1.2")

# #Firewall aktivieren
# firewall_rule = azure_native.sql.FirewallRule("firewallRule",
#     end_ip_address="255.255.255.255",
#     firewall_rule_name="firewallruleALL",
#     resource_group_name=resource_group.name,
#     server_name=server.name,
#     start_ip_address="0.0.0.0")


# #Database
# database_dwh = azure_native.sql.Database("database",
#     database_name="DWH",
#     #kind="v12.0,user,vcore,serverless",
#     resource_group_name=resource_group.name,
#     server_name=server.name,
#     sku=azure_native.sql.SkuArgs(
#         capacity=6,
#         family="Gen5",
#         name="GP_S_Gen5",
#         tier="GeneralPurpose"
#     ),
#     collation="SQL_Latin1_General_CP1_CI_AS",
#     catalog_collation="SQL_Latin1_General_CP1_CI_AS",
#     auto_pause_delay=60,
#     min_capacity=1,
#     requested_backup_storage_redundancy="Local"
#     )

# database_source = azure_native.sql.Database("dbsource1",
#     database_name="DBSource1",
#     #kind="v12.0,user,vcore,serverless",
#     resource_group_name=resource_group.name,
#     server_name=server.name,
#     sku=azure_native.sql.SkuArgs(
#         capacity=6,
#         family="Gen5",
#         name="GP_S_Gen5",
#         tier="GeneralPurpose"
#     ),
#     collation="SQL_Latin1_General_CP1_CI_AS",
#     auto_pause_delay=60,
#     min_capacity=1,
#     catalog_collation="SQL_Latin1_General_CP1_CI_AS",
#     requested_backup_storage_redundancy="Local",
#     sample_name="AdventureWorksLT"
#     )
