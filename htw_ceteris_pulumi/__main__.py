"""An Azure RM Python Pulumi program"""

#main.py is the Pulumi program that defines our stack resources.
import time
import pulumi
import pulumi_azure_native as azure_native
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
import asyncio

## do we need $logs older

my_bool=True
meta_table=[]
#linked service names
linked_service_name_sql="LS_ASQL_DB"
linked_service_name_blob ="LS_ABLB_CSV"
linked_service_name_datalake ="LS_ADLS_Target"

## Create an Azure Resource Group
resource_group = infra.createResourceGroup(cfg.resourceGroupName)
## Create Storage Account
### Storage Account for Source  
account_source =  infra.createStorageAccout(cfg.storageAccountSourceName, resource_group)
### Storage Account for Destination
account_destination = infra.createStorageAccout(cfg.storageAccountDestinationName, resource_group)
##  Create Blob Container for Source
blob_container = infra.createBlobContainer(cfg.blobContainerName,resource_group, account_source) #'htw_container1'
##  Create Blob Container for Import
blob_container_import = infra.createBlobContainer("import",resource_group, account_destination) #'htw_container1'#cfg var
##  Create Blob Container for Archiv
blob_container_archiv = infra.createBlobContainer('archiv',resource_group, account_destination) #'htw_container1'#cfg var
## Create Data Factory
data_factory = infra.createDataFactory(cfg.factoryName, resource_group) #'htwDataFactory'
## Create Server
server = infra.createServer(cfg.serverName,resource_group,cfg.dbSourceUserName,cfg.dbSourcePSW)#'htwServer'
## Create Firewall Rule
firewall_rule = infra.createFirewallRule(resource_group, server,cfg.firewallName)
## Create Database Source
db_source= infra.createDatabaseSource(resource_group,server,cfg.dbSourceName,"AdventureWorksLT")
## Create Datawarehouse Database (Target)
db_target= infra.createDatabaseTarget(resource_group, server,cfg.dbDWHName)



### Store Storage Account Key for Source
key_storage_account_source = infra.getAccountStorageKey(account_source,resource_group)
## Store Storage Account Key for Destination
key_storage_account_destination = infra.getAccountStorageKey(account_destination,resource_group)

# #meta_table = db.meta_table_100.copy() #db.get_meta_table(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW)
# df_meta_table = pd.DataFrame(db.meta_table_100)
# print(df_meta_table)

pulumi.Output.all(server.name,db_source.name) \
        .apply(lambda args:{
            db.create_sample(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            db.create_system_tables(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            db.fill_watermark_table(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            db.fill_meta_table(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            db.create_stored_procedure_watermark(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            db.create_stored_procedure_error_log(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW)
        })
#db.initDBSource(server,db_source,cfg.dbSourceUserName,cfg.dbSourcePSW)
#meta_table = db.get_meta_table(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW)
#print(meta_table)
# create a dataframe object from dict
#df_meta_table = pd.DataFrame(meta_table)
#print(df_meta_table)
# create linked services
linked_service_sql_db2 = createLSSourceASQLandReturn(data_factory.name,linked_service_name_sql,server.name,"1433",db_source.name,cfg.dbSourceUserName,cfg.dbSourcePSW,resource_group.name)
linked_service_blob =createLSABLBandReturn(data_factory.name,linked_service_name_blob,account_source.name, key_storage_account_source,resource_group.name)
linked_service_datalake = createLSTargetADLSandReturn(data_factory.name,linked_service_name_datalake,account_destination.name, key_storage_account_destination,resource_group.name)

# Create datasets
### Create dataset for whole Database
dataset_asql = createDatasetASQLAndReturn("DB",data_factory,linked_service_sql_db2,resource_group)
meta_table= db.meta_table_10.copy()#db.get_meta_table(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW) #db.meta_table_10.copy()
df_meta_table = pd.DataFrame(meta_table)
#print(df_meta_table)

### CREATE DATASETs FOR EACH CSV FILE (NAME OF CSV FILE SHOULD BE EQUAL TO NAME IN SQL TABLE)
#### save table names for csv tables
table_names_csv=df_meta_table.loc[df_meta_table['table_type']=="CSV"]['table_name'].values.tolist()

datasets_blob_csv_auto=[]

for table_name_csv in table_names_csv:
    # create dataset csv
    dataset_blob_csv=createDatasetABLBAndReturn(table_name_csv,data_factory,linked_service_blob,resource_group)
    # add dataflow into data_flows_auto list
    datasets_blob_csv_auto.append({'table_name': table_name_csv,
                            'dataset_obj': dataset_blob_csv
                            })
### CREATE DATASETs ARVHIV, IMPORT AND TEMP FOLDERS IN BLOB STORAGE
dataset_dl_archiv = createDatasetADLSAndReturn("Archiv",data_factory,linked_service_datalake,resource_group)
dataset_dl_import = createDatasetADLSAndReturn("Import",data_factory,linked_service_datalake,resource_group)
dataset_dl_temp = createDatasetADLSAndReturn("Temp",data_factory,linked_service_datalake,resource_group)


# create dataflows
data_flows_auto=[]

for x in meta_table:
    # create dataflows
    data_flow = createDataFlowAndReturn(x['table_name'],x['key_column'],data_factory,resource_group,linked_service_datalake,dataset_dl_temp)
    # add dataflow into data_flows_auto list
    data_flows_auto.append({'table_name':x['table_name'],
                            'data_flow_obj': data_flow
                            })

# CREATE PIPELINES
## deltaload params
delta_load = pipe.Param("deltaload", None, "Bool")
delta_load_default = pipe.Param("deltaload", False, "Bool")

## csv params
csv_source_type = "delimitedtext"
csv_sink_type = "parquet"

## sql params
sql_sink_type = "parquet"
sql_source_type = "azuresql"
error_sp = "[dbo].[usp_update_error_table]" # cfg.var
wm_sp = "[dbo].[usp_write_watermark]" # cfg.var
archiv_source_type = "parquet"
archiv_sink_type = "parquet"

## create schema list to iterate between different schemas
df_schema_list = df_meta_table['table_schema'].loc[df_meta_table['table_schema']!='Manual'].unique()
#print(df_schema_list)

## create pipelines for sql tables
for schema in df_schema_list:
    #print(schema)
    table_names_sql=df_meta_table.loc[df_meta_table['table_schema']==schema]['table_name'].values.tolist()
    #print(table_names_sql)
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
                                                            [delta_load_default],
                                                            data_factory.name,
                                                            resource_group.name) 
## create pipelines for csv tables
csv_pipelines=[]
for dataset in datasets_blob_csv_auto:
    csv_pipelines.append(pipe.create_custom_csv_source_pipelines(dataset['table_name'],
                                                           dataset['dataset_obj'].name,
                                                           csv_sink_type,
                                                           csv_source_type,
                                                           linked_service_sql_db2.name,
                                                           error_sp,
                                                           dataset_dl_archiv.name,
                                                           dataset_dl_temp.name,
                                                           archiv_source_type,
                                                           archiv_sink_type,
                                                           resource_group.name,
                                                           data_factory.name)) 



#master pipeline
csv_exe_activities = pipe.create_custom_exe_activities(csv_pipelines)
sql_exe_activities = pipe.create_custom_exe_activities(sql_pipelines)
activities_all = sql_exe_activities+csv_exe_activities
activities_40 = [activities_all[x:x+40] for x in range(0, len(activities_all), 40)]
pipelines=[]

# create sub-master pipelines
for i, activities in enumerate(activities_40):
    name="PL_Import_Master_"+str(i+1)
    pipeline_master = pipe.create_pipeline(resource_name=name,
                                  factory_name=data_factory.name,
                                  resource_group_name=resource_group.name,
                                  pipeline_name=name,
                                  folder="Master",
                                  parameters=[delta_load],
                                  activities=activities
                                  )
    pipelines.append({'pipeline_name': name,
                               'pipeline_obj': pipeline_master
                            })

exe_pipelines = pipe.create_custom_exe_activities(pipelines)

# create master pipeline
pipeline_master = pipe.create_pipeline(resource_name="PL_Import_Master",
                                  factory_name=data_factory.name,
                                  resource_group_name=resource_group.name,
                                  pipeline_name="PL_Import_Master",
                                  folder="Master",
                                  parameters=[delta_load],
                                  activities=exe_pipelines
                                  )