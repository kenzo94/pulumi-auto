import pulumi
import pandas as pd
import htw_pulumi_linked_service as ls
import htw_pulumi_data_flow as dfl
import htw_pulumi_dataset as ds
import htw_pulumi_db as db
import htw_pulumi_pipelines as pipe
import htw_config as cfg


def createADFElements(resource_group, account_source, account_destination, data_factory, server, db_source,key_storage_account_source,key_storage_account_destination):
    """
    This function creates all adf elements needed to create pipeline followed by creation of pipelined

    :param resource_group obj: created resource goup object
    :param account_source obj: created source storage account object (stores csv files)
    :param account_destination obj: created target storage account object (stores temp(used to store new data in .parquet format), import(used as datalake) and archiv files)
    :param data_factory obj: created data factory object (used for creation of all relevant pipelines elements)
    :param server obj: created server object
    :param db_source obj: created database for source data (Meta Table for pipelines creation)
    :param key_storage_account_source obj: used to access source storage account 
    :param key_storage_account_destination obj: used to access target storage account 
    
    :return void

    :pulumi docs: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/
    """
    ## INITIALISE META TABLE
    meta_table=[]
    
    ## INITIALISE LINKED SERVICES NAMES
    linked_service_name_sql="LS_ASQL_DB"
    linked_service_name_blob ="LS_ABLB_CSV"
    linked_service_name_datalake ="LS_ADLS_Target"
    
    ## SAVE META TABLE DATA INTO LIST AND THE TRANSFORM INTO SAVE DATAFRAME
    ### Save Meta Data from DB (if Timedout error occures, please start pulumi up again)
    meta_table= db.get_meta_table(cfg.serverName,cfg.dbSourceName,cfg.dbSourceUserName,cfg.dbSourcePSW) 
    print(meta_table)
    ### Create dataframe for meta table
    df_meta_table = pd.DataFrame(meta_table)

    ## CREATE LINKED SERVICES
    ### Linked Service for Source Database 
    linked_service_sql_db2 = ls.createLSSourceASQLandReturn(data_factory,linked_service_name_sql,server,"1433",db_source,cfg.dbSourceUserName,cfg.dbSourcePSW,resource_group)
    ### Linked Service for Sorce Blob Storage 
    linked_service_blob =ls.createLSABLBandReturn(data_factory,linked_service_name_blob,account_source, key_storage_account_source,resource_group)
    ### Linked Service for Target Blob Storage 
    linked_service_datalake = ls.createLSTargetADLSandReturn(data_factory,linked_service_name_datalake,account_destination, key_storage_account_destination,resource_group)

    ## CREATE DATASETS
    ### CREATE DATASET FOR WHOLE DB
    dataset_asql = ds.createDatasetASQLAndReturn("DB",data_factory,linked_service_sql_db2,resource_group)

    ### CREATE DATASETs FOR EACH CSV FILE (NAME OF CSV FILE SHOULD BE EQUAL TO NAME IN SQL META_TABLE )
    #### save table names for csv tables
    table_names_csv=df_meta_table.loc[df_meta_table['table_type']=="CSV"]['table_name'].values.tolist()
    #### Initialize List for saving dictionary of datasets
    datasets_blob_csv_auto=[]

    #### Iterate through Names of CSV Files listed in SQL META_TABLE
    for table_name_csv in table_names_csv:
        ###### create dataset csv object
        dataset_blob_csv = ds.createDatasetABLBAndReturn(table_name_csv,"CSV",cfg.blobContainerName,data_factory,linked_service_blob,resource_group)
        ###### add dataset into datasets_blob_csv_auto list
        datasets_blob_csv_auto.append({'table_name': table_name_csv,
                                'dataset_obj': dataset_blob_csv
                                })

    ### CREATE DATASETs ARVHIV, IMPORT AND TEMP FOLDERS IN TARGET BLOB STORAGE
    #### Create Archiv Continer Dataset put it into DataLake folder
    dataset_dl_archiv = ds.createDatasetADLSAndReturn("Archiv","DataLake",data_factory,linked_service_datalake,resource_group)
    #### Create Import Continer Dataset put it into DataLake folder
    dataset_dl_import = ds.createDatasetADLSAndReturn("Import","DataLake",data_factory,linked_service_datalake,resource_group)
    #### Create Temp Continer Dataset put it into DataLake folder
    dataset_dl_temp = ds.createDatasetADLSAndReturn("Temp","DataLake",data_factory,linked_service_datalake,resource_group)


    ## CREATE DATAFLOWS
    data_flows_auto=[]

    ### Interate through elements in SQL META_TABLE
    for x in meta_table:
        #### create dataflow object
        data_flow = dfl.createDataFlowAndReturn(x['table_name'],x['key_column'],data_factory,resource_group,linked_service_datalake,dataset_dl_temp)
        #### add dataflow into data_flows_auto list
        data_flows_auto.append({'table_name':x['table_name'],
                                'data_flow_obj': data_flow
                                })

    ## CREATE PIPELINES

    ### deltaload params
    delta_load = pipe.Param("deltaload", None, "Bool")
    delta_load_default = pipe.Param("deltaload", False, "Bool")

    ### csv params
    csv_source_type = "delimitedtext"
    csv_sink_type = "parquet"

    ### sql params
    sql_sink_type = "parquet"
    sql_source_type = "azuresql"
    archiv_source_type = "parquet"
    archiv_sink_type = "parquet"

    ### create schema list to iterate between different schemas
    df_schema_list = df_meta_table['table_schema'].loc[df_meta_table['table_schema']!='Manual'].unique()

    ### create pipelines for sql tables
    for schema in df_schema_list:
        table_names_sql=df_meta_table.loc[df_meta_table['table_schema']==schema]['table_name'].values.tolist()
        sql_pipelines = pipe.create_custom_sql_source_pipelines(table_names_sql, 
                                                                dataset_asql.name,
                                                                sql_sink_type,
                                                                sql_source_type,
                                                                linked_service_sql_db2.name,
                                                                schema,
                                                                cfg.dbStpError,
                                                                cfg.dbStpWatermark,
                                                                dataset_dl_archiv.name,
                                                                dataset_dl_temp.name,
                                                                archiv_source_type,
                                                                archiv_sink_type,
                                                                [delta_load_default],
                                                                data_factory.name,
                                                                resource_group.name) 
    ### create pipelines for csv tables
    csv_pipelines=[]
    for dataset in datasets_blob_csv_auto:
        csv_pipelines.append(pipe.create_custom_csv_source_pipelines(dataset['table_name'],
                                                            dataset['dataset_obj'].name,
                                                            csv_sink_type,
                                                            csv_source_type,
                                                            linked_service_sql_db2.name,
                                                            cfg.dbStpError,
                                                            dataset_dl_archiv.name,
                                                            dataset_dl_temp.name,
                                                            archiv_source_type,
                                                            archiv_sink_type,
                                                            resource_group.name,
                                                            data_factory.name)) 



    ## CREATE MASTER PIPELINE
    ### Save Activities for CSV and SQL Source
    csv_exe_activities = pipe.create_custom_exe_activities(csv_pipelines)
    sql_exe_activities = pipe.create_custom_exe_activities(sql_pipelines)
    activities_all = sql_exe_activities+csv_exe_activities

    ### Split list of activities into sub-lists, each max. 40 activities
    activities_40 = [activities_all[x:x+40] for x in range(0, len(activities_all), 40)]
    pipelines=[]

    ### Create sub-master pipelines
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
    #### save activities for sub-master pipelines
    exe_pipelines = pipe.create_custom_exe_activities(pipelines)

    ### Create master pipelines, which consists of sub-master activities
    pipeline_master = pipe.create_pipeline(resource_name="PL_Import_Master",
                                    factory_name=data_factory.name,
                                    resource_group_name=resource_group.name,
                                    pipeline_name="PL_Import_Master",
                                    folder="Master",
                                    parameters=[delta_load],
                                    activities=exe_pipelines
                                    )

