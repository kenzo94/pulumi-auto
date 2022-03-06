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
import htw_pulumi_db as db
import htw_pulumi_pipelines as pipe
import htw_pulumi_infrastructure as infra
import pandas as pd
import htw_config as cfg

# await by infra creation server and ressource group
# Master Pipeline should consist of sub-master pipelines (limit 40)
# Stored procedure creation pyodbc


# Create Infrastructur
# infra.createFullInfraHTWProject(cfg.resourceGroupName,
#                                 cfg.storageAccountSourceName,
#                                 cfg.storageAccountDestinationName,
#                                 cfg.blobContainerName,
#                                 cfg.factoryName,
#                                 cfg.serverName,
#                                 cfg.dbSourceUserName,
#                                 cfg.dbSourcePSW,
#                                 cfg.firewallName,
#                                 cfg.dbSourceName,
#                                 cfg.dbDWHName)


## Create an Azure Resource Group
resource_group = infra.createResourceGroup(cfg.resourceGroupName)
    ## Create Storage Account
    ### Storage Account for Source  
account_source =  infra.createStorageAccout(cfg.storageAccountSourceName, resource_group.name)
#     ### Storage Account for Destination
account_destination = infra.createStorageAccout(cfg.storageAccountDestinationName, resource_group.name)
#     ##  Create Blob Container for Source
blob_container = infra.createBlobContainer(cfg.blobContainerName,resource_group.name, account_source.name) #'htw_container1'
#     ## Create Data Factory
data_factory = infra.createDataFactory(cfg.factoryName, resource_group.name) #'htwDataFactory'
#     ## Create Server
server = infra.createServer(cfg.serverName,resource_group.name,cfg.dbSourceUserName,cfg.dbSourcePSW)#'htwServer'
#     ## Create Firewall Rule
firewall_rule = infra.createFirewallRule(resource_group.name, server.name,cfg.firewallName)
#     ## Create Database Source
db_source= infra.createDatabaseSource(resource_group.name,server.name,cfg.dbSourceName,"AdventureWorksLT")
#     ## Create Datawarehouse Database (Target)
db_target= infra.createDatabaseTarget(resource_group.name, server.name,cfg.dbDWHName)#'htwDWH'

# ### Store Storage Account Key for Source
key_storage_account_source = infra.getAccountStorageKey(cfg.storageAccountSourceName,cfg.resourceGroupName)
# ### Store Storage Account Key for Destination
key_storage_account_destination = infra.getAccountStorageKey(cfg.storageAccountDestinationName,cfg.resourceGroupName)

# # create sample tables add retry method or try catch
db.create_sample(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW)
db.create_system_tables(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW)
#db.create_stored_procedure_watermark(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW)
#db.create_stored_procedure_error_log(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW)
# # create stored procedures
# # fill watermark table
db.fill_watermark_table(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW)
# load meta_table
meta_table = db.get_meta_table(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW)
# create a dataframe object from dict
df_meta_table = pd.DataFrame(meta_table)


#linked service names
linked_service_name_sql="LS_ASQL_DB"
linked_service_name_blob ="LS_ABLB_CSV"
linked_service_name_datalake ="LS_ADLS_Target"

# create linked services
linked_service_sql_db2 = createLSSourceASQLandReturn(cfg.factoryName,linked_service_name_sql,cfg.serverName,"1433",cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW,cfg.resourceGroupName)
linked_service_blob =createLSABLBandReturn(cfg.factoryName,linked_service_name_blob,cfg.storageAccountSourceName, key_storage_account_source,cfg.resourceGroupName)
linked_service_datalake = createLSTargetADLSandReturn(cfg.factoryName,linked_service_name_datalake,cfg.storageAccountDestinationName, key_storage_account_destination,cfg.resourceGroupName)

# create datasets
"""CREATE DATASET FOR WHOLE DATABASE"""
dataset_asql = createDatasetASQLAndReturn("DB",cfg.factoryName,linked_service_sql_db2,cfg.resourceGroupName)

"""CREATE DATASETs FOR EACH CSV FILE (NAME OF CSV FILE SHOULD BE EQUAL TO NAME IN SQL TABLE)"""
# save table names for csv tables
table_names_csv=df_meta_table.loc[df_meta_table['table_type']=="CSV"]['table_name'].values.tolist()

datasets_blob_csv_auto=[]

for table_name_csv in table_names_csv:
    # create dataset csv
    dataset_blob_csv=createDatasetABLBAndReturn(table_name_csv,cfg.factoryName,linked_service_blob,cfg.resourceGroupName)
    # add dataflow into data_flows_auto list
    datasets_blob_csv_auto.append({'table_name': table_name_csv,
                            'dataset_obj': dataset_blob_csv
                            })
"""CREATE DATASETs ARVHIV, IMPORT AND TEMP FOLDERS IN BLOB STORAGE"""
dataset_dl_archiv = createDatasetADLSAndReturn("Archiv",cfg.factoryName,linked_service_datalake,cfg.resourceGroupName)
dataset_dl_import = createDatasetADLSAndReturn("Import",cfg.factoryName,linked_service_datalake,cfg.resourceGroupName)
dataset_dl_temp = createDatasetADLSAndReturn("Temp",cfg.factoryName,linked_service_datalake,cfg.resourceGroupName)

# create dataflows

data_flows_auto=[]

for x in meta_table:
    # create dataflows
    data_flow = createDataFlowAndReturn(x['table_name'],x['key_column'],cfg.factoryName,cfg.resourceGroupName,linked_service_datalake)
    # add dataflow into data_flows_auto list
    data_flows_auto.append({'table_name':x['table_name'],
                            'data_flow_obj': data_flow
                            })

"""CREATE PIPELINES"""
# deltaload params
delta_load = pipe.Param("deltaload", None, "Bool")
delta_load_default = pipe.Param("deltaload", False, "Bool")

#csv params
csv_source_type = "delimitedtext"
csv_sink_type = "parquet"

# sql params
sql_sink_type = "parquet"
sql_source_type = "azuresql"
error_sp = "[dbo].[UpdateErrorTable]"
wm_sp = "[dbo].[usp_write_watermark]"
archiv_source_type = "parquet"
archiv_sink_type = "parquet"


df_schema_list = df_meta_table['table_schema'].loc[df_meta_table['table_schema']!='Manual'].unique()
print(df_schema_list)

for schema in df_schema_list:
    print(schema)
    table_names_sql=df_meta_table.loc[df_meta_table['table_schema']==schema]['table_name'].values.tolist()
    print(table_names_sql)
    sql_pipelines = pipe.create_custom_sql_source_pipelines(table_names_sql, 
                                                            dataset_asql.name,
                                                            sql_sink_type,
                                                            sql_source_type,
                                                            linked_service_sql_db2.name,
                                                            schema,
                                                            error_sp,
                                                            wm_sp,
                                                            dataset_dl_archiv.name,
                                                            dataset_dl_temp.name,
                                                            archiv_source_type,
                                                            archiv_sink_type,
                                                            [delta_load_default]) 

csv_pipeline_names=[]
for dataset in datasets_blob_csv_auto:
    csv_pipeline_names.append(pipe.create_custom_csv_source_pipelines(dataset['table_name'],
                                                           dataset['dataset_obj'].name,
                                                           csv_sink_type,
                                                           csv_source_type,
                                                           linked_service_sql_db2.name,
                                                           error_sp,
                                                           dataset_dl_archiv.name,
                                                           dataset_dl_temp.name,
                                                           archiv_source_type,
                                                           archiv_sink_type)) 



# master pipeline
csv_exe_activities = pipe.create_custom_exe_activities(csv_pipeline_names)
sql_exe_activities = pipe.create_custom_exe_activities(sql_pipelines)
activities_all = sql_exe_activities+csv_exe_activities
activities_40 = [activities_all[x:x+40] for x in range(0, len(activities_all), 40)]
print(activities_40)

for activities in activities_40:
    pipeline_master = pipe.create_pipeline(resource_name="PL_Import_Master",
                                  factory_name=cfg.factoryName,
                                  resource_group_name=cfg.resourceGroupName,
                                  pipeline_name="PL_Import_Master",
                                  folder="Master",
                                  parameters=[delta_load],
                                  activities=activities
                                  )


# pipeline_master = pipe.create_pipeline(resource_name="PL_Import_Master",
#                                   factory_name=cfg.factoryName,
#                                   resource_group_name=cfg.resourceGroupName,
#                                   pipeline_name="PL_Import_Master",
#                                   folder="Master",
#                                   parameters=[delta_load],
#                                   activities=sql_exe_activities+csv_exe_activities
#                                   )
