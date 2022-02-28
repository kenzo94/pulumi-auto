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
from htw_pulumi_db import createsample
import htw_pulumi_pipelines as pipe
import htw_pulumi_infrastructure as infra
import pandas as pd
#Gedanken über Benamung
#import pulumi_azure as Azure

#ConfigDatenbank Basic Tier 2-3 tabellen aufnehmen (Linked Service)

#Hanna: Pipeline erst ohne Parameter



# load meta_table
meta_table = getMetaTable()
# create a dataframe object from dict
df_meta_table = pd.DataFrame(meta_table)

#create sample tables
createsample()

# # Blob auto
# blob_account_name_auto = "plsource1"#account_source.name
# blob_account_key_auto = "amu31xt5j91azbjZsACM5VaDMIIEnIj8Y3aYOKmX1aCbdLRCJxI/lLNpbno/X1nKHlPxyYfi3v3aSakEKB6Gpw=="

# # Datalake auto
# data_lake_account_name_auto = "pldestination"#account_dest.name
# data_lake_account_key_auto = "SOKULBYhq4QRX6XeVDKZTnoYffi9iPZeEmKqLArrt+5gPl+MP+46AOp3V6r/wBxq2jEK+xiZd87v1/XJILwk1A=="

# #linked service names
# linked_service_name_sql="LS_ASQL_DB"
# linked_service_name_blob ="LS_ABLB_CSV"
# linked_service_name_datalake ="LS_ADLS_Target"

# # create linked services
# linked_service_sql_db2 = createLSSourceASQLandReturn(factory.name,linked_service_name_sql,server.name,"1433",database_source.name,infra.getuserid(),infra.getpassword(),resource_group.name)
# linked_service_blob =createLSABLBandReturn(factory.name,linked_service_name_blob,account_source.name, blob_account_key_auto,resource_group.name)
# linked_service_datalake = createLSTargetADLSandReturn(factory.name,linked_service_name_datalake,account_dest.name, data_lake_account_key_auto,resource_group.name)

# # create datasets
# """CREATE DATASET FOR WHOLE DATABASE"""
# dataset_asql = createDatasetASQLAndReturn("DB",factory.name,linked_service_sql_db2,resource_group.name)

# """CREATE DATASETs FOR EACH CSV FILE (NAME OF CSV FILE SHOULD BE EQUAL TO NAME IN SQL TABLE)"""
# # save table names for csv tables
# table_names_csv=df_meta_table.loc[df_meta_table['table_type']=="CSV"]['table_name'].values.tolist()

# datasets_blob_csv_auto=[]

# for table_name_csv in table_names_csv:
#     # create dataset csv
#     dataset_blob_csv=createDatasetABLBAndReturn(table_name_csv,factory.name,linked_service_blob,resource_group.name)
#     # add dataflow into data_flows_auto list
#     datasets_blob_csv_auto.append({'table_name': table_name_csv,
#                             'dataset_obj': dataset_blob_csv
#                             })
# """CREATE DATASETs ARVHIV, IMPORT AND TEMP FOLDERS IN BLOB STORAGE"""
# dataset_dl_archiv = createDatasetADLSAndReturn("Archiv",factory.name,linked_service_datalake,resource_group.name)
# dataset_dl_import = createDatasetADLSAndReturn("Import",factory.name,linked_service_datalake,resource_group.name)
# dataset_dl_temp = createDatasetADLSAndReturn("Temp",factory.name,linked_service_datalake,resource_group.name)

# # create dataflows

# data_flows_auto=[]

# for x in meta_table:
#     # create dataflows
#     data_flow = createDataFlowAndReturn(x['table_name'],x['key_column'],factory.name,resource_group.name,linked_service_datalake)
#     # add dataflow into data_flows_auto list
#     data_flows_auto.append({'table_name':x['table_name'],
#                             'data_flow_obj': data_flow
#                             })

# """CREATE PIPELINES"""
# # deltaload params
# delta_load = pipe.Param("deltaload", None, "Bool")
# delta_load_default = pipe.Param("deltaload", False, "Bool")

# #csv params
# csv_source_type = "delimitedtext"
# csv_sink_type = "parquet"

# # sql params
# sql_dataset = "DS_ASQL_DB"
# sql_sink_type = "parquet"
# sql_source_type = "azuresql"
# error_sp = "[dbo].[UpdateErrorTable]"
# wm_sp = "[dbo].[usp_write_watermark]"
# archiv_source_type = "parquet"
# archiv_sink_type = "parquet"


# df_schema_list = df_meta_table['table_schema'].loc[df_meta_table['table_schema']!='Manual'].unique()
# print(df_schema_list)

# for schema in df_schema_list:
#     print(schema)
#     table_names_sql=df_meta_table.loc[df_meta_table['table_schema']==schema]['table_name'].values.tolist()
#     print(table_names_sql)
#     sql_pipelines = pipe.create_custom_sql_source_pipelines(table_names_sql, 
#                                                             dataset_asql.name,
#                                                             sql_sink_type,
#                                                             sql_source_type,
#                                                             linked_service_sql_db2.name,
#                                                             schema,
#                                                             error_sp,
#                                                             wm_sp,
#                                                             dataset_dl_archiv.name,
#                                                             dataset_dl_temp.name,
#                                                             archiv_source_type,
#                                                             archiv_sink_type,
#                                                             [delta_load_default]) 

# csv_pipeline_names=[]
# for dataset in datasets_blob_csv_auto:
#     csv_pipeline_names.append(pipe.create_custom_csv_source_pipelines(dataset['table_name'],
#                                                            dataset['dataset_obj'].name,
#                                                            csv_sink_type,
#                                                            csv_source_type,
#                                                            linked_service_sql_db2.name,
#                                                            error_sp,
#                                                            dataset_dl_archiv.name,
#                                                            dataset_dl_temp.name,
#                                                            archiv_source_type,
#                                                            archiv_sink_type)) 


# # master pipeline
# csv_exe_activities = pipe.create_custom_exe_activities(csv_pipeline_names)
# sql_exe_activities = pipe.create_custom_exe_activities(sql_pipelines)
# pipeline_master = pipe.create_pipeline(resource_name="PL_Import_Master",
#                                   factory_name=factory_name_auto,
#                                   resource_group_name=resource_name_auto,
#                                   pipeline_name="PL_Import_Master",
#                                   folder="Master",
#                                   parameters=[delta_load],
#                                   activities=sql_exe_activities+csv_exe_activities
#                                   )
