#htw_pulumi_infrastructure.py defices the pulumi inftastructure 

import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import storage
from pulumi_azure_native import resources

psw="OZh2fwL3TUqSzFO0fwfc"
userid="Team4Admin"

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
    administrator_login=userid,
    administrator_login_password=psw, #später lösen, sodass sie nicht mehr im sourcecode sind
    resource_group_name=resource_group.name,
    server_name="htw-cet-sqlserver",
    public_network_access = "Enabled",
    minimal_tls_version="1.2")

#Firewall aktivieren
firewall_rule = azure_native.sql.FirewallRule("firewallRule",
    end_ip_address="255.255.255.255",
    firewall_rule_name="firewallruleALL",
    resource_group_name=resource_group.name,
    server_name=server.name,
    start_ip_address="0.0.0.0")


#Database
database_dwh = azure_native.sql.Database("database",
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

database_source = azure_native.sql.Database("dbsource1",
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
