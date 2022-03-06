#htw_pulumi_infrastructure.py defices the pulumi inftastructure 

import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import storage
from pulumi_azure_native import resources
import asyncio

# Create an Azure Resource Group This Pulumi program creates an Azure resource group and storage account and then exports the storage account’s primary key.
def createResourceGroup(resource_group_name):
    resource_group =  resources.ResourceGroup(resource_group_name,
        resource_group_name=resource_group_name)
    return resource_group

# Create an Azure resource (Storage Account - Storage and Source)
def createStorageAccout(account_name, resource_group_name):
    account = storage.StorageAccount(account_name,   
        account_name=account_name,
        resource_group_name=resource_group_name,
        sku=storage.SkuArgs(
            name=storage.SkuName.STANDARD_LRS,
        ),
        kind=storage.Kind.STORAGE_V2)
    return account

def createBlobContainer(blob_container_name ,resource_group_name,account_name):
    blob_container = azure_native.storage.BlobContainer(blob_container_name,
        account_name=account_name,
        container_name= blob_container_name,
        resource_group_name=resource_group_name)
    return blob_container

def createDataFactory(data_factory_name,resource_group_name):
    factory = azure_native.datafactory.Factory(data_factory_name,
        factory_name = data_factory_name,
        resource_group_name=resource_group_name)
    return factory


def createServer(server_name, resource_group_name, dbSourceUserName, dbSourcePSW):
    server = azure_native.sql.Server(server_name,
        server_name=server_name,
        administrator_login=dbSourceUserName,
        administrator_login_password=dbSourcePSW,
        resource_group_name=resource_group_name,
        public_network_access = "Enabled",
        minimal_tls_version="1.2")
    return server


# #Firewall aktivieren
def createFirewallRule(resource_group_name, server_name, firewall_rule_name):
    firewall_rule = azure_native.sql.FirewallRule(firewall_rule_name,
        end_ip_address="255.255.255.255",
        resource_group_name=resource_group_name,
        server_name=server_name,
        start_ip_address="0.0.0.0")
    return firewall_rule

# #Database
def createDatabaseTarget(resource_group_name, server_name, database_name):
    database =azure_native.sql.Database(database_name,
        database_name=database_name,
        resource_group_name=resource_group_name,
        server_name=server_name,
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
def createDatabaseSource(resource_group_name,server_name,database_name, sample_name):
    database = azure_native.sql.Database(database_name,
        database_name=database_name,
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
    return database 

def getAccountStorageKey(account_name,resource_group_name):
    account_keys = storage.list_storage_account_keys(account_name=account_name,resource_group_name=resource_group_name)
    account_key= account_keys.keys[0]['value']
    print(account_name+" key is "+account_key)
    return account_key

#Infratsurcture als Methode
def createFullInfraHTWProject(resource_group_name,account_name_source,account_name_destination,blob_container_name,factory_name,server_name,db_source_user_name,db_source_psw,firewall_name,db_source_name,db_dwh_name):
    ## Create an Azure Resource Group
    resource_group = createResourceGroup(resource_group_name)
    ## Create Storage Account
    ### Storage Account for Source  
    account_source =  createStorageAccout(account_name_source, resource_group.name)
    ### Storage Account for Destination
    account_destination = createStorageAccout(account_name_destination, resource_group.name)
    ##  Create Blob Container for Source
    blob_container = createBlobContainer(blob_container_name,resource_group.name, account_source.name) #'htw_container1'
    ## Create Data Factory
    data_factory = createDataFactory(factory_name, resource_group.name) #'htwDataFactory'
    ## Create Server
    server = createServer(server_name,resource_group.name,db_source_user_name,db_source_psw)#'htwServer'
    ## Create Firewall Rule
    firewall_rule = createFirewallRule(resource_group.name, server.name,firewall_name)
    ## Create Database Source
    db_source= createDatabaseSource(resource_group.name,server.name,db_source_name,"AdventureWorksLT")
    ## Create Datawarehouse Database (Target)
    db_target= createDatabaseTarget(resource_group.name, server.name,db_dwh_name)#'htwDWH'