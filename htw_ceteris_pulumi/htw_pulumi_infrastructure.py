#htw_pulumi_infrastructure.py defices the pulumi inftastructure 
#https://www.pulumi.com/docs/intro/concepts/resources/options/dependson/
import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import storage
from pulumi_azure_native import resources
import os


def createResourceGroup(resource_group_name):
    """
    :param resource_group_name string: name of resource group from htw_config.py file (should be unique among stacks)
    
    :return created resource_group object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/resources/resourcegroup/
    """
    resource_group =  resources.ResourceGroup(resource_group_name,
        resource_group_name=resource_group_name)
    return resource_group



def saveLocalFilesIntoBlobStorage(account,resource_group,account_name,blob_container_name,local_path,pattern):
    """
    this function waits for resource group and account to be created and then saves local files into named container in the selected account blob storage

    :param account obj: storage account object, where desired container locates
    :param resource_group obj: resource group object, where desired account locates
    :param account_name string: storage account name, where desired container locates    
    :param blob_container_name string: container name, where desired files should be uploaded
    :param local_path string: relative path to the folder, where files are located in the project
    :param pattern string: pattern of the files, which need to be imported       
    
    :return void

    :pulumi docs: https://docs.microsoft.com/de-de/cli/azure/storage/blob?view=azure-cli-latest#az-storage-blob-upload-batch
    """
    pulumi.Output.all(resource_group.name,account.name) \
        .apply(lambda args: storage.list_storage_account_keys(
            resource_group_name=args[0],
            account_name=args[1]
        )).apply(lambda accountKeys:
        os.system(f"az storage blob upload-batch -d https://{account_name}.blob.core.windows.net/{blob_container_name} -s {local_path} --pattern {pattern} --account-key {accountKeys.keys[0].value}"))



def getAccountStorageKey(account,resource_group):
    """
    this function waits for resource group and account to be created and then retreives account keys for selected account in specific resource group

    :param account obj: storage account object 
    :param resource_group obj: resource group objects     
    
    :return account_key object

    :pulumi docs: https://docs.microsoft.com/de-de/cli/azure/storage/blob?view=azure-cli-latest#az-storage-blob-upload-batch
    """
    account_key=pulumi.Output.all(resource_group.name,account.name) \
        .apply(lambda args: storage.list_storage_account_keys(
            resource_group_name=args[0],
            account_name=args[1]
        )).apply(lambda accountKeys: accountKeys.keys[0].value)
    return account_key


def createStorageAccout(account_name, resource_group): 
    """
    this function waits for resource group to be created and then creates storage account
    
    :param account_name string: desired storage account name from htw_config.py file (should be unique among stacks; max. name length 24 char)
    :param resource_group obj: resource group object, where desired storage account should be created    
    
    :return account object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/storage/storageaccount/
    """
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
    """
    this function waits for resource group and storage account to be created and then creates blob container
    
    :param blob_container_name string: desired blob container name from htw_config.py file
    :param resource_group obj: resource group object, where storage account is located
    :param account obj: storage account object, where desired blob container should be created    
    
    :return blob_container object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/storage/blobcontainer/
    """
    blob_container = azure_native.storage.BlobContainer(blob_container_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group,account]), 
        account_name=account.name,
        container_name= blob_container_name,
        resource_group_name=resource_group.name)
    return blob_container

def createDataFactory(data_factory_name,resource_group):
    """
    this function waits for resource group to be created and then creates data factory object
    
    :param data_factory_name string: desired data factory name from htw_config.py file (should be unique among stacks)
    :param resource_group obj: resource group object, where data fatory should be created  
    
    :return factory object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/factory/
    """
    factory = azure_native.datafactory.Factory(data_factory_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group]), 
        factory_name = data_factory_name,
        resource_group_name=resource_group.name)
    return factory


def createServer(server_name, resource_group, dbSourceUserName, dbSourcePSW):
    """
    this function waits for resource group to be created and then creates server object
    
    :param server_name string: desired server name from htw_config.py file (should be unique among stacks)
    :param resource_group obj: resource group object, where server should be created  
    :param dbSourceUserName string: admin user name from from htw_config.py file
    :param dbSourcePSW string: admin user password from from htw_config.py file     

    :return server object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/sql/server/
    """
    server = azure_native.sql.Server(server_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group]), 
        server_name=server_name,
        administrator_login=dbSourceUserName,
        administrator_login_password=dbSourcePSW,
        resource_group_name=resource_group.name,
        public_network_access = "Enabled",
        minimal_tls_version="1.2")
    return server


def createFirewallRule(resource_group, server, firewall_rule_name):
    """
    this function waits for resource group and server to be created and then creates firewall rule. 
    This firewall rule enables all ip adresses to conect to server remotely.
    
    :param firewall_rule_name string: desired firewall name from htw_config.py file
    :param server obj: server object, where firewall rule should be created  
    :param resource_group obj: resource group object, where server is located  

    :return firewall_rule object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/sql/firewallrule/
    """
    firewall_rule = azure_native.sql.FirewallRule(firewall_rule_name,
        opts = pulumi.ResourceOptions(depends_on=[resource_group,server]), 
        end_ip_address="255.255.255.255",
        resource_group_name=resource_group.name,
        server_name=server.name,
        start_ip_address="0.0.0.0")
    return firewall_rule


def createDatabaseTarget(resource_group, server, database_name):
    """
    this function waits for resource group and server to be created and then creates target database. 
    
    :param database_name string: desired database name from htw_config.py file (should be unique among stacks)
    :param server obj: server object, where database should be created  
    :param resource_group obj: resource group object, where server is located  

    :return firewall_rule object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/sql/database/
    """
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
    

def createDatabaseSource(resource_group,server,database_name, sample_name):
    """
    this function waits for resource group and server to be created and then creates source database with sample data. 
    
    :param database_name string: desired database name from htw_config.py file (should be unique among stacks)
    :param server obj: server object, where database should be created  
    :param resource_group obj: resource group object, where server is located
    :param sample_name string: name of available sample data ("AdventureWorksLT")

    :return firewall_rule object

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/sql/database/
    """
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
        sample_name=sample_name)
    return database
