"""An Azure RM Python Pulumi program"""

import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import datafactory


factory_name_auto = "htwcetdatafactory"
resource_name_auto = "pulumiauto"
dsreftype = "DatasetReference"
dfreftype = "DataFlowReference"
lsreftype = "LinkedServiceReference"
pipreftype = "PipelineReference"
succeeded =["Succeeded"]
failed=["Failed"]
suc_fail=["Succeeded", "Failed"]

# class

class Mapping:

    def __init__(self,
                 source_name: str,
                 source_type: str,
                 source_physical_type: str,
                 sink_name: str,
                 sink_type: str,
                 sink_physical_type: str):

        self.source_name = source_name
        self.source_type = source_type
        self.source_physical_type = source_physical_type
        self.sink_name = sink_name
        self.sink_type = sink_type
        self.sink_physical_type = sink_physical_type


class Param:

    def __init__(self, name_: str, value_: str, type_: str):
        self.name = name_
        self.value = value_
        self.type = type_

class Variable:
    
    def __init__(self, name: str, type_: str, default_:str) -> None:
        if type not in ["String", "Boolean", "Array"]:
            raise ValueError("Possible Types: String, Boolean, Array")
        self.name = name
        self.type = type_
        self.default = default_

class DataflowDatasetParam:

    def __init__(self, dataflow_activity_name: str, param_names: list, param_values: list):
        if len(param_names) != len(param_values):
            raise ValueError("Length mismatch of Parameter names and values.")
        self.dataflow_activity_name = dataflow_activity_name
        self.param_names = param_names
        self.param_values = param_values
        

# help methods

# [Mapping()]
def mapping(pairs: list):
    mapping_list = []

    if (not pairs):
        return None

    for obj in pairs:
        map = {
            "source": {
                "name": obj.source_name,
                "type": obj.source_type,
                "physicalType": obj.source_physical_type
            },
            "sink": {
                "name": obj.sink_name,
                "type": obj.sink_type,
                "physicalType": obj.sink_physical_type
            }
        }
        mapping_list.append(map)
    return {"mappings": mapping_list}

def create_mapping(source_list: list, s_type_list: list, s_physical_type_list:list,
                   sink_list:list, si_type_list:list, si_physical_type_list:list):
    mapping_list = []
    for source, s_type_, s_pt, sink, si_type, si_pt in \
            zip(source_list, s_type_list, s_physical_type_list, sink_list, si_type_list, si_physical_type_list):
        m = Mapping(source, s_type_, s_pt, sink, si_type, si_pt)
        mapping_list.append(m)
    return mapping_list

# sp param [Param()]
def spParam(params: list):
    dict = {}

    if (not params):
        return None

    for param in params:
        if "@" in param.value:
            dict[param.name] = {
                "value": {
                    "value": param.value,
                    "type": "Expression"
                },
                "type": param.type
            }
        else:
            dict[param.name] = {
                "value": param.value,
                "type": param.type
            }
    return dict

# [Param()]
def param(params: list):
    dict = {}
    
    if (not params): 
        return None
    
    for param in params:
        dict[param.name] = {
                "value": param.value,
                "type": param.type
            }
    return dict

def variable(vars: list):
    dict = {}
    
    if not vars:
        return None
    
    for var in vars:
        dict[var.name] = {
            "type": var.type,
            "defaultValue": var.default
        }
    return dict

#[DataflowDatasetParam()]
def df_ds_param(params: list):
    dict = {}
    if not params:
        return None

    for param in params:
        for names, values in zip(param.param_names, param.param_values):
            if param.dataflow_activity_name not in dict.keys():
                dict[param.dataflow_activity_name] = {
                    names: values
                }
            elif param.dataflow_activity_name in dict.keys():
                dict[param.dataflow_activity_name][names] = values
    return dict

#logerror
def create_sp_error_param(activity: str, source: str, sink: str):
    sp_names = ["DataFactory_Name", "Destination", "ErrorCode", "ErrorDescription", "ErrorLoggedTime",
                "Execution_Status", "FailureType", "Pipeline_Name", "RunId", "Sink_Type", "Source", "Source_Type"]
    sp_types = ["String", "String", "String", "String", "String", "String", "String", "String",
                "String", "String", "String", "String"]
    sp_values = ["@pipeline().DataFactory", sink, "@{activity('"+activity+"').error.errorCode}",
                 "@{activity('"+activity+"').error.Message}", "@utcnow()", "@{activity('"+activity+"').output.executionDetails[0].status}",
                 "@concat(activity('"+activity+"').error.message,'failureType:',activity('"+activity+"').error.failureType)",
                 "@pipeline().Pipeline", "@pipeline().RunId", "@{activity('"+activity+"').output.executionDetails[0].sink.type}",
                 source, "@{activity('"+activity+"').output.executionDetails[0].source.type}"]
    param = []
    
    for name_, value_, type_ in zip(sp_names, sp_values, sp_types):
        p = Param(name_, value_, type_)
        param.append(p)
        
    return param

# type = DatasetReference
# parameters = {param_name: {value, type}}
def dataset_ref(reference_name: str, type_: str, parameters: list = None):
    if (not reference_name and not type_ and not parameters):
        return None

    input = datafactory.DatasetReferenceArgs(
        reference_name=reference_name,
        type=type_,  # DatasetReference
        parameters=param(parameters)
    )
    return [input]


def dataflow_ref(reference_name: str, type_: str, dataset_parameters: list = None, parameters: list = None):
    ref = datafactory.DataFlowReferenceArgs(
        reference_name=reference_name,
        type=type_,
        dataset_parameters=df_ds_param(dataset_parameters),
        parameters=param(parameters)
    )
    return ref


# type : LinkedServiceReference
def linked_service_reference(reference_name: str, type_: str, parameters: list = None):
    if (not reference_name and not type_ and not parameters):
        return None

    input = datafactory.LinkedServiceReferenceArgs(
        reference_name=reference_name,
        type=type_,  # LinkedServiceReference
        parameters=param(parameters)
    )

    return input

# cond: "Succeeded" Or "Failed" list
def depend_on(activity: str, cond: list):
    if (not activity and not cond):
        return None
    
    dep = datafactory.ActivityDependencyArgs(
            activity=activity,
            dependency_conditions=cond
         )
    return dep


def pipeline_ref(pipeline_name: str, pipeline_type: str, ref_name: str = None):
    pipeline_ref = datafactory.PipelineReferenceArgs(
        reference_name=pipeline_name,
        type=pipeline_type,
        name=ref_name
    )
    return pipeline_ref


# Only standard params for source and sink
def parquetsink():
    sink = datafactory.ParquetSinkArgs(
        type="ParquetSink",
        store_settings=datafactory.AzureBlobFSWriteSettingsArgs(
            type="AzureBlobFSWriteSettings"
        ),
        format_settings=datafactory.ParquetWriteSettingsArgs(
            type="ParquetWriteSettings"
        )
    )
    return sink

def parquetsource():
    source = datafactory.ParquetSourceArgs(
        type="ParquetSource",
        store_settings=datafactory.AzureBlobFSReadSettingsArgs(
            type="AzureBlobFSReadSettings"
        ),
        disable_metrics_collection = True
    )
    return source


def delimitedsource():
    source = datafactory.DelimitedTextSourceArgs(
        type="DelimitedTextSource",
        store_settings=datafactory.AzureBlobStorageReadSettingsArgs(
            type="AzureBlobStorageReadSettings"
        ),
        format_settings=datafactory.DelimitedTextReadSettingsArgs(
            type="DelimitedTextReadSettings"
        )
    )
    return source


def dbsource(query: str):
    if not query:
        source = datafactory.AzureSqlSourceArgs(
            type="AzureSqlSource",
            sql_reader_query=query
        )
        return source
    else:
        source = datafactory.AzureSqlSourceArgs(
            type="AzureSqlSource",
        )
        return source


def sinks(sink_type: str):
    switch = {
        "parquet": parquetsink()
    }
    return switch.get(sink_type, "Sink type should be: parquet, ...")


def sources(source_type: str, query: str = None):
    switch = {
        "parquet": parquetsource(),
        "delimitedtext": delimitedsource(),
        "azuresql": dbsource(query)
    }
    return switch.get(source_type, "Source type should be: parquet, delimetedtext or azuresql.")

# compute_type = [‘General’, ‘MemoryOptimized’, ‘ComputeOptimized’]
# core_count = 8, 16, 32, 48, 80, 144 and 272
def compute(compute_type: str, core_count: int):
    if (not compute_type and not core_count): return None
    
    c = datafactory.ExecuteDataFlowActivityTypePropertiesResponseComputeArgs(
        compute_type=compute_type,
        core_count=core_count
    )
    return c

def staging(folder_path: str, linked_service: datafactory.LinkedServiceReferenceArgs):
    if (not folder_path and not linked_service): return None
    
    s = datafactory.DataFlowStagingInfoArgs(
        folder_path=folder_path,
        linked_service=linked_service
    )
    return s

def foldername(foldername: str):
    if not foldername:
        return None
    
    f = datafactory.PipelineFolderArgs(name=foldername)
    return f

def redirect_rows(linked_service_name: str, path: str):
    if not linked_service_name and not path:
        return None
    
    settings=datafactory.RedirectIncompatibleRowSettingsArgs(
        linked_service_name=linked_service_name,
        path=path
    )
    return settings

def skip_error_file(data_inconsistency: bool, file_missing: bool):
    settings=datafactory.SkipErrorFileArgs(
        data_inconsistency=data_inconsistency,
        file_missing=file_missing
    )
    return settings

# Activities
def create_ExecutePipelineActivity(name: str,
                                   pipeline_ref_name: str,
                                   pipeline_ref_type: str,
                                   depends_on: list = None,
                                   description: str = None,
                                   parameters: list = None,
                                   wait_on_completion: bool = None):

    activity = datafactory.ExecutePipelineActivityArgs(
        name=name,
        type="ExecutePipeline",
        pipeline=pipeline_ref(pipeline_ref_name, pipeline_ref_type),
        depends_on=depends_on,
        description=description,
        parameters=param(parameters),
        wait_on_completion=wait_on_completion
    )

    return activity


def create_CopyActivity(name: str,
                        sink: str, # source type 
                        source: str, #source type
                        sql_query: str = None, # sql query for source
                        data_integration_units: int = None,
                        depends_on: list = None,
                        enable_skip_incompatible_row: bool = False,
                        enable_staging: bool = False,
                        inputs_source: str = None, # source dataset reference
                        inputs_source_param: list = None, # expect list of param objects
                        inputs_source_type: str = None,
                        linked_service_name: str = None, # not needed anymore
                        linked_service_type: str = None,
                        linked_service_param: list = None,
                        outputs_sink: str = None, # sink dataset reference
                        outputs_sink_param: list = None,
                        outputs_sink_type: str = None,
                        parallel_copies: int = None,
                        preserve: list = None, #Needed?
                        preserve_rules: list = None, #needed?
                        redirect_incompatible_row_settings_linked_service_name: str = None, # must be specified if enable_skip_incompatible_row is true
                        redirect_incompatible_row_settings_path: str = None,
                        translator: list = None, # expect list of mapping objects
                        skip_error_file_data_inconsistency: bool = False,
                        skip_error_file_file_missing: bool = True,
                        validate_data_consistency: bool = False):

    activity = datafactory.CopyActivityArgs(
        name=name,
        type="Copy",
        sink=sinks(sink),
        source=sources(source, sql_query),
        data_integration_units=data_integration_units,
        depends_on=depends_on,
        enable_skip_incompatible_row=enable_skip_incompatible_row,
        enable_staging=enable_staging,
        inputs=dataset_ref(inputs_source, inputs_source_type, inputs_source_param),
        linked_service_name=linked_service_reference(linked_service_name, linked_service_type, linked_service_param),
        outputs=dataset_ref(outputs_sink, outputs_sink_type,outputs_sink_param),
        parallel_copies=parallel_copies,
        preserve=preserve,
        preserve_rules=preserve_rules,
        redirect_incompatible_row_settings=redirect_rows(redirect_incompatible_row_settings_linked_service_name, redirect_incompatible_row_settings_path),
        skip_error_file=skip_error_file(skip_error_file_data_inconsistency, skip_error_file_file_missing),
        translator=mapping(translator),
        validate_data_consistency=validate_data_consistency
    )

    return activity


def create_spActivity(linked_service_name: str,
                      linked_service_type: str,
                      name: str,
                      stored_procedure_name: str,
                      linked_service_param: list = None,
                      depends_on: list = None,
                      stored_procedure_parameters: list = None
                      ):

    activity = datafactory.SqlServerStoredProcedureActivityArgs(
        linked_service_name=linked_service_reference(
            linked_service_name, linked_service_type, linked_service_param),
        name=name,
        stored_procedure_name=stored_procedure_name,
        type="SqlServerStoredProcedure",
        depends_on=depends_on,
        stored_procedure_parameters=spParam(stored_procedure_parameters),
    )

    return activity


def create_ifActivity(expression_type: str,
                      expression_value: str,
                      name: str,
                      depends_on_activity: str = None,
                      depends_on_con: str = None,
                      if_false_activities: list = None,
                      if_true_activities: list = None
                      ):
    pass


def create_dfActivity(df_ref_name: str,
                      df_ref_type: str,
                      name: str,
                      dataset_param: list =None,
                      df_param: list = None,
                      compute_type: str = None,
                      compute_core_count: int= None,
                      continue_on_error: bool = None,
                      depends_on: list = None,
                      linked_service_name: str = None,
                      linked_service_type: str = None,
                      linked_service_param: dict = None,
                      run_concurrently: bool = None,
                      staging_folder_path: str = None,
                      staging_linked_service_name: str = None,
                      staging_linked_service_type: str = None,
                      staging_linked_service_param: list = None,
                      trace_level:str = None):
    
    activity = datafactory.ExecuteDataFlowActivityArgs(
        data_flow=dataflow_ref(df_ref_name, df_ref_type,dataset_param, df_param),
        type="ExecuteDataFlow",
        name=name,
        compute=compute(compute_type, compute_core_count),
        continue_on_error=continue_on_error,
        depends_on=depends_on,
        linked_service_name=linked_service_reference(linked_service_name, linked_service_type, param(linked_service_param)),
        run_concurrently=run_concurrently,
        staging=staging(staging_folder_path, linked_service_reference(staging_linked_service_name, staging_linked_service_type, staging_linked_service_param)),
        trace_level=trace_level #‘coarse’, ‘fine’, and ‘none’
    )
    return activity


def create_lkActivity(ds_ref_name: str,
                      ds_ref_type: str,
                      name: str,
                      source: str, 
                      sql_query: str = None,
                      ds_ref_param: list = None,
                      depends_on: list = None,
                      first_row_only: bool = True,
                      linked_service_name: str = None, # not needed anymore
                      linked_service_type: str = None,
                      linked_service_param: list = None
                      ):
    
    activity = datafactory.LookupActivityArgs(
        dataset=dataset_ref(ds_ref_name, ds_ref_type,ds_ref_param),
        name=name,
        type="Lookup",
        source=sources(source, sql_query),
        depends_on=depends_on,
        first_row_only=first_row_only,
        linked_service_name=linked_service_reference(linked_service_name, linked_service_type, linked_service_param)
    )
    return activity

# Pipeline
# API Reference: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/pipeline/


def create_pipeline(resource_name: str,
                    factory_name: str,
                    resource_group_name: str,
                    pipeline_name: str = None,
                    activities: list = None,
                    concurrency: int = None,
                    folder: str = None,
                    parameters: list = None,
                    variables: list = None):

    pipeline = datafactory.Pipeline(resource_name=resource_name,
                                    pipeline_name=pipeline_name,
                                    factory_name=factory_name,
                                    resource_group_name=resource_group_name,
                                    activities=activities,
                                    concurrency=concurrency,
                                    folder=foldername(folder),
                                    parameters=param(parameters),
                                    variables=variable(variables)
                                    )
    return pipeline


#Pipeline PL_Import_DimABLBEmail
#activities

#mapping
source_list = ["Login email", "Identifier", "First name", "Last name"]
s_type_list = ["String", "String", "String", "String"]
s_physical_type_list = ["UTF8", "UTF8", "UTF8", "UTF8"]
sink_list = ["Loginemail", "Identifier", "Firstname", "Lastname"]
si_type_list = ["String", "String", "String", "String"]
si_physical_type_list = ["UTF8", "UTF8", "UTF8", "UTF8"]

#params
delta_load = Param("deltaload", None, "Bool")
filename_param = Param("filename", "Email.parquet", "Expression")
email_archive_filename = Param("filename", "@concat('Email_',utcnow(),'.parquet')", "Expression")
df_temp_to_import_param = DataflowDatasetParam("SRCADLTempEmail", ["filename"], ["Email.parquet"])

copy_email_to_temp = create_CopyActivity(name="ABLBEmailToADLTempEmail",
                                       sink="parquet",
                                       source="delimitedtext",
                                       inputs_source="DS_ABLB_Email",
                                       inputs_source_type=dsreftype,
                                       outputs_sink="DS_ADLS_Temp",
                                       outputs_sink_type=dsreftype,
                                       outputs_sink_param=[filename_param],
                                       translator=create_mapping(source_list, s_type_list, s_physical_type_list, sink_list, si_type_list, si_physical_type_list)
                                       )

df_temp_to_import = create_dfActivity(df_ref_name="DF_Import_ADLSTempEmail", 
                                      df_ref_type=dfreftype,
                                      name="DF_Import_ADLTempEmail",
                                      dataset_param=[df_temp_to_import_param],
                                      depends_on=[depend_on(copy_email_to_temp.name, succeeded)])

sp_error_log1 = create_spActivity(linked_service_name="LS_ASQL_SalesLT",
                                 linked_service_type=lsreftype,
                                 name="ASQL_prc_CDUpdateErrorTable",
                                 stored_procedure_name="[dbo].[UpdateErrorTable]",
                                 stored_procedure_parameters=create_sp_error_param(copy_email_to_temp.name, "Source", "Temp"),
                                 depends_on=[depend_on(copy_email_to_temp.name, failed)]
                                 ) 

sp_error_log2 = create_spActivity(linked_service_name="LS_ASQL_SalesLT",
                                 linked_service_type=lsreftype,
                                 name="ASQL_prc_DFUpdateErrorTable",
                                 stored_procedure_name="[dbo].[UpdateErrorTable]",
                                 stored_procedure_parameters=create_sp_error_param(df_temp_to_import.name, "Temp", "Import"),
                                 depends_on=[depend_on(df_temp_to_import.name, failed)]
                                 ) 

copy_email_to_archiv = create_CopyActivity(name="ABLBTempEmailToADLArchivEmail",
                                       sink="parquet",
                                       source="parquet",
                                       inputs_source="DS_ADLS_Temp",
                                       inputs_source_type=dsreftype,
                                       inputs_source_param=[filename_param],
                                       outputs_sink="DS_ADLS_Archiv",
                                       outputs_sink_type=dsreftype,
                                       outputs_sink_param=[email_archive_filename],
                                       translator=create_mapping(sink_list, si_type_list, si_physical_type_list, sink_list, si_type_list, si_physical_type_list),
                                       depends_on=[depend_on(df_temp_to_import.name, succeeded)]
                                       )

pipeline_PL_Import_DimABLBEmail = create_pipeline(resource_name="PL_Import_DimABLBEmail",
                                                pipeline_name="PL_Import_DimABLBEmail",
                                                resource_group_name=resource_name_auto,
                                                factory_name=factory_name_auto,
                                                folder="Import",
                                                activities=[copy_email_to_temp, df_temp_to_import, sp_error_log1,
                                                            sp_error_log2, copy_email_to_archiv])

#master
exe_PL_Import_DimABLBEmail = create_ExecutePipelineActivity(name="PL_Import_DimABLBEmail_WoC",
                                                            pipeline_ref_name="PL_Import_DimABLBEmail",
                                                            pipeline_ref_type=pipreftype,
                                                            wait_on_completion=True)

pipeline_master = create_pipeline(resource_name="PL_Import_Master",
                                  factory_name=factory_name_auto,
                                  resource_group_name=resource_name_auto,
                                  pipeline_name="PL_Import_Master",
                                  folder="Master",
                                  parameters=[delta_load],
                                  activities=[exe_PL_Import_DimABLBEmail]
                                  )