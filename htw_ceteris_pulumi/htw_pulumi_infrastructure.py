#htw_pulumi_infrastructure.py defices the pulumi inftastructure 
#https://www.pulumi.com/docs/intro/concepts/resources/options/dependson/
import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import storage
from pulumi_azure_native import resources
import htw_pulumi_db as db
import htw_config as cfg

account_keys_list=[]

def emptyAccountKeysList():
    account_keys_list=[]

# Create an Azure Resource Group This Pulumi program creates an Azure resource group and storage account and then exports the storage account’s primary key.
def createResourceGroup(resource_group_name):
    resource_group =  resources.ResourceGroup(resource_group_name,
        resource_group_name=resource_group_name)
    return resource_group
# def saveAccountStorageKey(account_name,resource_group_name):
#     account_keys = storage.list_storage_account_keys(
#         account_name=account_name,
#         resource_group_name=resource_group_name)
#     account_key= account_keys.keys[0]['value']
#     print(f"{account_name} key is {account_key}")
#     account_keys_list.append({'account_name': account_name,
#                         'key':account_key})

def getAccountStorageKey(account,resource_group):
    account_key=pulumi.Output.all(resource_group.name,account.name) \
        .apply(lambda args: storage.list_storage_account_keys(
            resource_group_name=args[0],
            account_name=args[1]
        )).apply(lambda accountKeys: accountKeys.keys[0].value)
    account_keys_list.append({'account_name': account.name,
                        'key':account_key})
    return account_key
    #pulumi.export(account_name+"_export",account_key)

# Create an Azure resource (Storage Account - Storage and Source)
def createStorageAccout(account_name, resource_group): 
    account = storage.StorageAccount(account_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group]),   
        account_name=account_name,
        resource_group_name=resource_group.name,
        sku=storage.SkuArgs(
            name=storage.SkuName.STANDARD_LRS,
        ),
        kind=storage.Kind.STORAGE_V2)
    return account

def createBlobContainer(blob_container_name ,resource_group,account):
    blob_container = azure_native.storage.BlobContainer(blob_container_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group,account]), 
        account_name=account.name,
        container_name= blob_container_name,
        resource_group_name=resource_group.name)
    return blob_container

def createDataFactory(data_factory_name,resource_group):
    factory = azure_native.datafactory.Factory(data_factory_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group]), 
        factory_name = data_factory_name,
        resource_group_name=resource_group.name)
    return factory


def createServer(server_name, resource_group, dbSourceUserName, dbSourcePSW):
    server = azure_native.sql.Server(server_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group]), 
        server_name=server_name,
        administrator_login=dbSourceUserName,
        administrator_login_password=dbSourcePSW,
        resource_group_name=resource_group.name,
        public_network_access = "Enabled",
        minimal_tls_version="1.2")
    return server


# #Firewall aktivieren
def createFirewallRule(resource_group, server, firewall_rule_name):
    firewall_rule = azure_native.sql.FirewallRule(firewall_rule_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group,server]), 
        end_ip_address="255.255.255.255",
        resource_group_name=resource_group.name,
        server_name=server.name,
        start_ip_address="0.0.0.0")
    return firewall_rule

# #Database
def createDatabaseTarget(resource_group, server, database_name):
    database =azure_native.sql.Database(database_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group,server]), 
        database_name=database_name,
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
        requested_backup_storage_redundancy="Local")

    return database
    

#Create Database Source
def createDatabaseSource(resource_group,server,database_name, sample_name):
    database = azure_native.sql.Database(database_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group,server]), 
        database_name=database_name,
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
        sample_name=sample_name) #"AdventureWorksLT"
    return database
