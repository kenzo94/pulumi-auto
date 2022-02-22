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
from htw_pulumi_db import getMetaTable
import htw_pulumi_pipelines as pipe
import pandas as pd
#Gedanken über Benamung
#import pulumi_azure as Azure

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

    #ConfigDatenbank Basic Tier 2-3 tabellen aufnehmen (Linked Service)

#Hanna: Pipeline erst ohne Parameter

# load meta_table
meta_table = getMetaTable()
# create a dataframe object from dict
df_meta_table = pd.DataFrame(meta_table)

# Blob auto
blob_account_name_auto = "plsource1"#account_source.name
blob_account_key_auto = "amu31xt5j91azbjZsACM5VaDMIIEnIj8Y3aYOKmX1aCbdLRCJxI/lLNpbno/X1nKHlPxyYfi3v3aSakEKB6Gpw=="

# Datalake auto
data_lake_account_name_auto = "pldestination"#account_dest.name
data_lake_account_key_auto = "SOKULBYhq4QRX6XeVDKZTnoYffi9iPZeEmKqLArrt+5gPl+MP+46AOp3V6r/wBxq2jEK+xiZd87v1/XJILwk1A=="

#linked service names
linked_service_name_sql="LS_ASQL_DB"
linked_service_name_blob ="LS_ABLB_CSV"
linked_service_name_datalake ="LS_ADLS_Target"

# create linked services
linked_service_sql_db2 = createLSSourceASQLandReturn(factory.name,linked_service_name_sql,server.name,"1433",database_source.name,userid,psw,resource_group.name)
linked_service_blob =createLSABLBandReturn(factory.name,linked_service_name_blob,account_source.name, blob_account_key_auto,resource_group.name)
linked_service_datalake = createLSTargetADLSandReturn(factory.name,linked_service_name_datalake,account_dest.name, data_lake_account_key_auto,resource_group.name)

# create datasets
"""CREATE DATASET FOR WHOLE DATABASE"""
dataset_asql = createDatasetASQLAndReturn("DB",factory.name,linked_service_sql_db2,resource_group.name)

"""CREATE DATASETs FOR EACH CSV FILE (NAME OF CSV FILE SHOULD BE EQUAL TO NAME IN SQL TABLE)"""
# save table names for csv tables
table_names_csv=df_meta_table.loc[df_meta_table['table_type']=="CSV"]['table_name'].values.tolist()

datasets_blob_csv_auto=[]

for table_name_csv in table_names_csv:
    # create dataset csv
    dataset_blob_csv=createDatasetABLBAndReturn(table_name_csv,factory.name,linked_service_blob,resource_group.name)
    # add dataflow into data_flows_auto list
    datasets_blob_csv_auto.append({'table_name': table_name_csv,
                            'dataset_obj': dataset_blob_csv
                            })
"""CREATE DATASETs ARVHIV, IMPORT AND TEMP FOLDERS IN BLOB STORAGE"""
dataset_dl_archiv = createDatasetADLSAndReturn("Archiv",factory.name,linked_service_datalake,resource_group.name)
dataset_dl_import = createDatasetADLSAndReturn("Import",factory.name,linked_service_datalake,resource_group.name)
dataset_dl_temp = createDatasetADLSAndReturn("Temp",factory.name,linked_service_datalake,resource_group.name)

# create dataflows

data_flows_auto=[]

for x in meta_table:
    # create dataflows
    data_flow = createDataFlowAndReturn(x['table_name'],x['key_column'],factory.name,resource_group.name,linked_service_datalake)
    # add dataflow into data_flows_auto list
    data_flows_auto.append({'table_name':x['table_name'],
                            'data_flow_obj': data_flow
                            })

"""CREATE PIPELINES"""
"""
df_schema_list = df_meta_table['table_schema'].loc[df_meta_table['table_schema']!='Manual'].unique()
print(df_schema_list)

for schema in df_schema_list:
    print(schema)
    table_names_sql=df_meta_table.loc[df_meta_table['table_schema']==schema]['table_name'].values.tolist()
    print(table_names_sql)
    pipe.create_custom_sql_source_pipelines(table_names_sql,dataset_asql.name,"parquet","azuresql",linked_service_sql_db2.name,schema,"[dbo].[UpdateErrorTable]","[dbo].[usp_write_watermark]",dataset_dl_archiv.name,dataset_dl_temp.name,"parquet","parquet") 
"""
"""for dataset in datasets_blob_csv_auto:
    pipe.create_custom_sql_source_pipelines([dataset['table_name']],dataset['dataset_obj'].name,"parquet","azuresql",linked_service_blob.name,"","[dbo].[UpdateErrorTable]","[dbo].[usp_write_watermark]",dataset_dl_archiv.name,dataset_dl_temp.name,"parquet","parquet") 
"""

