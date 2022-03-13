"""An Azure RM Python Pulumi program"""

import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import datafactory
import htw_config as cfg
import pandas as pd

# https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-2017&tabs=ssms adventure works sample databases

dsreftype = "DatasetReference"
dfreftype = "DataFlowReference"
lsreftype = "LinkedServiceReference"
pipreftype = "PipelineReference"
succeeded = ["Succeeded"]
failed = ["Failed"]
suc_fail = ["Succeeded", "Failed"]

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

    def __init__(self, name: str, type_: str, default_: str) -> None:
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


def create_mapping(source_list: list, s_type_list: list, s_physical_type_list: list,
                   sink_list: list, si_type_list: list, si_physical_type_list: list):
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


def param(params: list, flag: int = None):
    dict = {}

    if not params and not flag:
        return None

    if flag == 1 and params:
        for param in params:
            dict[param.name] = {
                "defaultValue": param.value,
                "type": param.type
            }
        return dict
    elif params:
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

# [DataflowDatasetParam()]


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

# logerror


def create_sp_error_param(activity: str, source: str, sink: str):
    sp_names = ["DataFactory_Name", "Destination", "ErrorCode", "ErrorDescription", "ErrorLoggedTime",
                "Execution_Status", "FailureType", "Pipeline_Name", "RunId", "Sink_Type", "Source", "Source_Type"]
    sp_types = ["String", "String", "String", "String", "String", "String", "String", "String",
                "String", "String", "String", "String"]
    sp_values = ["@pipeline().DataFactory", sink, "@{activity('"+activity+"').error.errorCode}",
                 "@{activity('"+activity+"').error.Message}", "@utcnow()", "@{activity('" +
                 activity+"').output.executionDetails[0].status}",
                 "@concat(activity('"+activity+"').error.message,'failureType:',activity('" +
                 activity+"').error.failureType)",
                 "@pipeline().Pipeline", "@pipeline().RunId", "@{activity('" +
                 activity+"').output.executionDetails[0].sink.type}",
                 source, "@{activity('"+activity+"').output.executionDetails[0].source.type}"]
    param = []

    for name_, value_, type_ in zip(sp_names, sp_values, sp_types):
        p = Param(name_, value_, type_)
        param.append(p)

    return param

# type = DatasetReference
# parameters = {param_name: {value, type}}, flag 0: Copyactivity 1: lookupactivity


def dataset_ref(reference_name: str, type_: str, parameters: list = None, flag: int = None):
    if (not reference_name and not type_ and not parameters and flag == 0):
        return None

    if flag == 0 and reference_name and type_:
        ref = datafactory.DatasetReferenceArgs(
            reference_name=reference_name,
            type=type_,  # DatasetReference
            parameters=param(parameters)
        )
        return [ref]
    elif flag == 1 and reference_name and type_:
        ref = datafactory.DatasetReferenceArgs(
            reference_name=reference_name,
            type=type_,  # DatasetReference
            parameters=param(parameters)
        )
        return ref


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
        disable_metrics_collection=True
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
    if query is not None:
        source = datafactory.AzureSqlSourceArgs(
            type="AzureSqlSource",
            sql_reader_query={
                    "value": query,
                    "type": "Expression"}
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
    if (not compute_type and not core_count):
        return None

    c = datafactory.ExecuteDataFlowActivityTypePropertiesResponseComputeArgs(
        compute_type=compute_type,
        core_count=core_count
    )
    return c


def staging(folder_path: str, linked_service: datafactory.LinkedServiceReferenceArgs):
    if (not folder_path and not linked_service):
        return None

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

    settings = datafactory.RedirectIncompatibleRowSettingsArgs(
        linked_service_name=linked_service_name,
        path=path
    )
    return settings


def skip_error_file(data_inconsistency: bool, file_missing: bool):
    settings = datafactory.SkipErrorFileArgs(
        data_inconsistency=data_inconsistency,
        file_missing=file_missing
    )
    return settings


def if_expression(type_: str, value: str):
    ex = datafactory.ExpressionArgs(
        type=type_,
        value=value
    )
    return ex


def create_filename_param(tablename: str):
    filename = Param("filename", f"{tablename}.parquet", "Expression")
    return filename


def create_archiv_filename_param(tablename: str):
    filename = Param(
        "filename", f"@concat('{tablename}_',utcnow(),'.parquet')", "Expression")
    return filename


def create_df_ds_param(tablename: str, param_names: list, param_values: list):
    param = DataflowDatasetParam(
        f"SRCADLTemp{tablename}", param_names, param_values)
    return param

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
                        sink: str,  # source type
                        source: str,  # source type
                        sql_query: str = None,  # sql query for source
                        data_integration_units: int = None,
                        depends_on: list = None,
                        enable_skip_incompatible_row: bool = False,
                        enable_staging: bool = False,
                        inputs_source: str = None,  # source dataset reference
                        inputs_source_param: list = None,  # expect list of param objects
                        inputs_source_type: str = None,
                        linked_service_name: str = None,  # not needed anymore
                        linked_service_type: str = None,
                        linked_service_param: list = None,
                        outputs_sink: str = None,  # sink dataset reference
                        outputs_sink_param: list = None,
                        outputs_sink_type: str = None,
                        parallel_copies: int = None,
                        preserve: list = None,  # Needed?
                        preserve_rules: list = None,  # needed?
                        # must be specified if enable_skip_incompatible_row is true
                        redirect_incompatible_row_settings_linked_service_name: str = None,
                        redirect_incompatible_row_settings_path: str = None,
                        translator: list = None,  # expect list of mapping objects
                        skip_error_file_data_inconsistency: bool = False,
                        skip_error_file_file_missing: bool = False,
                        validate_data_consistency: bool = False):

    activity = datafactory.CopyActivityArgs(
        name=name[:55],
        type="Copy",
        sink=sinks(sink),
        source=sources(source, sql_query),
        data_integration_units=data_integration_units,
        depends_on=depends_on,
        enable_skip_incompatible_row=enable_skip_incompatible_row,
        enable_staging=enable_staging,
        inputs=dataset_ref(inputs_source, inputs_source_type,
                           inputs_source_param, 0),
        linked_service_name=linked_service_reference(
            linked_service_name, linked_service_type, linked_service_param),
        outputs=dataset_ref(outputs_sink, outputs_sink_type,
                            outputs_sink_param, 0),
        parallel_copies=parallel_copies,
        preserve=preserve,
        preserve_rules=preserve_rules,
        redirect_incompatible_row_settings=redirect_rows(
            redirect_incompatible_row_settings_linked_service_name, redirect_incompatible_row_settings_path),
        skip_error_file=skip_error_file(
            skip_error_file_data_inconsistency, skip_error_file_file_missing),
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
                      depends_on: list = None,
                      description: str = None,
                      if_false_activities: list = None,
                      if_true_activities: list = None):
    activity = datafactory.IfConditionActivityArgs(
        expression=if_expression(expression_type, expression_value),
        name=name,
        type="IfCondition",
        depends_on=depends_on,
        description=description,
        if_false_activities=if_false_activities,
        if_true_activities=if_true_activities,
    )
    return activity


def create_dfActivity(df_ref_name: str,
                      df_ref_type: str,
                      name: str,
                      dataset_param: list = None,
                      df_param: list = None,
                      compute_type: str = None,
                      compute_core_count: int = None,
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
                      trace_level: str = None):

    activity = datafactory.ExecuteDataFlowActivityArgs(
        data_flow=dataflow_ref(df_ref_name, df_ref_type,
                               dataset_param, df_param),
        type="ExecuteDataFlow",
        name=name,
        compute=compute(compute_type, compute_core_count),
        continue_on_error=continue_on_error,
        depends_on=depends_on,
        linked_service_name=linked_service_reference(
            linked_service_name, linked_service_type, param(linked_service_param)),
        run_concurrently=run_concurrently,
        staging=staging(staging_folder_path, linked_service_reference(
            staging_linked_service_name, staging_linked_service_type, staging_linked_service_param)),
        trace_level=trace_level  # ‘coarse’, ‘fine’, and ‘none’
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
                      linked_service_name: str = None,
                      linked_service_type: str = None,
                      linked_service_param: list = None
                      ):

    activity = datafactory.LookupActivityArgs(
        dataset=dataset_ref(ds_ref_name, ds_ref_type, ds_ref_param, 1),
        name=name,
        type="Lookup",
        source=sources(source, sql_query),
        depends_on=depends_on,
        first_row_only=first_row_only,
        linked_service_name=linked_service_reference(
            linked_service_name, linked_service_type, linked_service_param)
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
 #                                   opts = pulumi.ResourceOptions(delete_before_replace=True),  
                                    pipeline_name=pipeline_name,
                                    factory_name=factory_name,
                                    resource_group_name=resource_group_name,
                                    activities=activities,
                                    concurrency=concurrency,
                                    folder=foldername(folder),
                                    parameters=param(parameters, 1),
                                    variables=variable(variables)
                                    )
    return pipeline

# custom pipelines
def create_custom_csv_source_pipelines(csv: str,
                                       csv_dataset: str,
                                       csv_dataset_sink_type: str,
                                       csv_dataset_source_type: str,
                                       sql_linked_service: str,
                                       error_sp: str,
                                       archiv_dataset: str,
                                       temp_dataset: str,
                                       archiv_source_type: str,
                                       archiv_sink_type: str,
                                       resource_name_auto,
                                       factory_name_auto):

    filename_param = create_filename_param(csv)
    email_archive_filename_param = create_archiv_filename_param(csv)
    df_temp_to_import_param = create_df_ds_param(
        csv, [filename_param.name], [filename_param.value])
    
    copy_email_to_temp = create_CopyActivity(name=f"ABLB{csv}ToADLTemp{csv}",
                                             sink=csv_dataset_sink_type,
                                             source=csv_dataset_source_type,
                                             inputs_source=csv_dataset,
                                             inputs_source_type=dsreftype,
                                             outputs_sink=temp_dataset,
                                             outputs_sink_type=dsreftype,
                                             outputs_sink_param=[
                                                 filename_param]
                                             )

    df_temp_to_import = create_dfActivity(df_ref_name=f"DF_Import_ADLSTemp{csv}",
                                          df_ref_type=dfreftype,
                                          name=f"DF_Import_ADLTemp{csv}",
                                          dataset_param=[
                                              df_temp_to_import_param],
                                          depends_on=[
                                              depend_on(copy_email_to_temp.name, succeeded)]
                                          )

    sp_error_log1 = create_spActivity(linked_service_name=sql_linked_service,
                                      linked_service_type=lsreftype,
                                      name="ASQL_prc_CDUpdateErrorTable",
                                      stored_procedure_name=error_sp,
                                      stored_procedure_parameters=create_sp_error_param(
                                          copy_email_to_temp.name, "Source", "Temp"),
                                      depends_on=[
                                          depend_on(copy_email_to_temp.name, failed)]
                                      )

    sp_error_log2 = create_spActivity(linked_service_name=sql_linked_service,
                                      linked_service_type=lsreftype,
                                      name="ASQL_prc_DFUpdateErrorTable",
                                      stored_procedure_name=error_sp,
                                      stored_procedure_parameters=create_sp_error_param(
                                          df_temp_to_import.name, "Temp", "Import"),
                                      depends_on=[
                                          depend_on(df_temp_to_import.name, failed)]
                                      )

    copy_temp_to_archiv = create_CopyActivity(name=f"ABLBTemp{csv}ToADLArchiv{csv}",
                                              sink=archiv_sink_type,
                                              source=archiv_source_type,
                                              inputs_source=temp_dataset,
                                              inputs_source_type=dsreftype,
                                              inputs_source_param=[
                                                  filename_param],
                                              outputs_sink=archiv_dataset,
                                              outputs_sink_type=dsreftype,
                                              outputs_sink_param=[
                                                  email_archive_filename_param],
                                              depends_on=[
                                                  depend_on(df_temp_to_import.name, succeeded)]
                                              )

    pipeline = create_pipeline(resource_name=f"PL_Import_DimABLB{csv}",
                               pipeline_name=f"PL_Import_DimABLB{csv}",
                               resource_group_name=resource_name_auto,
                               factory_name=factory_name_auto,
                               folder="Import",
                               activities=[copy_email_to_temp, df_temp_to_import, sp_error_log1,
                                           sp_error_log2, copy_temp_to_archiv]
                               )
    #f"PL_Import_DimABLB{csv}"
    pipeline_dict = {'pipeline_name': f"PL_Import_DimABLB{csv}",
                     'pipeline_obj': pipeline
                    }
    return pipeline_dict


def create_custom_sql_source_pipelines(tablenames: list,
                                       sql_dataset: str,
                                       sql_dataset_sink_type: str,
                                       sql_dataset_source_type: str,
                                       sql_linkedservice: str,
                                       schema: str,
                                       error_sp: str,
                                       wm_sp: str,
                                       archiv_dataset: str,
                                       temp_dataset: str,
                                       archiv_source_type: str,
                                       archiv_sink_type: str,
                                       pipeline_params: list,
                                       factory_name_auto,
                                       resource_name_auto):

    pipeline_names = []

    for tablename in tablenames:

        filename_param = create_filename_param(tablename)  # dataset param
        archive_filename_param = create_archiv_filename_param(
            tablename)  # dataset param
        df_ds_param = create_df_ds_param(tablename, [filename_param.name], [
                                         filename_param.value])  # dataset param

        # #if false

        copy_to_temp = create_CopyActivity(name=f"ASQL{schema+tablename}AllToADLTemp{tablename}",
                                           sink=sql_dataset_sink_type,  # parquet
                                           source=sql_dataset_source_type,  # "azuresql"
                                           sql_query=f"select * from {schema}.{tablename}",
                                           inputs_source=sql_dataset,  # "DS_ASQL_DB"
                                           inputs_source_type=dsreftype,
                                           outputs_sink=temp_dataset,  # "DS_ADLS_Temp"
                                           outputs_sink_type=dsreftype,
                                           outputs_sink_param=[filename_param]
                                           )

        sp_error_log1 = create_spActivity(linked_service_name=sql_linkedservice,  # "LS_ASQL_SalesLT"
                                          linked_service_type=lsreftype,
                                          name="prc_CDAllUpdateErrorTable",
                                          # "[dbo].[UpdateErrorTable]"
                                          stored_procedure_name=error_sp,
                                          stored_procedure_parameters=create_sp_error_param(
                                              copy_to_temp.name, "Source", "Temp"),
                                          depends_on=[
                                              depend_on(copy_to_temp.name, failed)]
                                          )

        # if true

        lk_Watermarktable = create_lkActivity(ds_ref_name=sql_dataset,
                                              ds_ref_type=dsreftype,
                                              name="DS_ASQL_dboWatermarktable_FRO",
                                              source=sql_dataset_source_type,
                                              sql_query=f"select * from WaterMarkTable where TableName='{tablename}'"
                                              )

        lk_table = create_lkActivity(ds_ref_name=sql_dataset,
                                     ds_ref_type=dsreftype,
                                     name=f"DS_ASQL_SalesLT{tablename}_FRO",
                                     source=sql_dataset_source_type,
                                     sql_query=f"select MAX(ModifiedDate) as NewWatermarkvalue from {schema}.{tablename}",
                                     )

        copy_to_temp_watermark = create_CopyActivity(name=f"ASQL{schema+tablename}ToADLTemp{tablename}",
                                                     sink=sql_dataset_sink_type,
                                                     source=sql_dataset_source_type,
                                                     sql_query="select * from "+schema+"."+tablename +
                                                     " where ModifiedDate> '@{activity('"+lk_Watermarktable.name+"').output.firstRow.WatermarkValue}' and ModifiedDate<= '@{activity('" +
                                                                                       lk_table.name+"').output.firstRow.NewWatermarkvalue}'",
                                                     inputs_source=sql_dataset,
                                                     inputs_source_type=dsreftype,
                                                     outputs_sink=temp_dataset,
                                                     outputs_sink_type=dsreftype,
                                                     outputs_sink_param=[
                                                         filename_param],
                                                     depends_on=[depend_on(lk_table.name, succeeded), depend_on(
                                                         lk_Watermarktable.name, succeeded)]
                                                     )

        sp_WriteWatermark = create_spActivity(linked_service_name=sql_linkedservice,
                                              linked_service_type=lsreftype,
                                              name="ASQL_prc_USPWriteWatermark",
                                              # "[dbo].[usp_write_watermark]"
                                              stored_procedure_name=wm_sp,
                                              stored_procedure_parameters=[Param("modifiedDate", "@{activity('"+lk_table.name+"').output.firstRow.NewWatermarkvalue}", "DateTime"),
                                                                           Param("TableName", "@{activity('"+lk_Watermarktable.name+"').output.firstRow.TableName}", "String")],
                                              depends_on=[
                                                  depend_on(copy_to_temp_watermark.name, succeeded)]
                                              )

        sp_error_log2 = create_spActivity(linked_service_name=sql_linkedservice,
                                          linked_service_type=lsreftype,
                                          name="ASQL_prc_CDUpdateErrorTable",
                                          stored_procedure_name=error_sp,
                                          stored_procedure_parameters=create_sp_error_param(
                                              copy_to_temp_watermark.name, "Source", "Temp"),
                                          depends_on=[
                                              depend_on(copy_to_temp_watermark.name, failed)]
                                          )
        # ende if condition

        # if activity
        if_deltaload = create_ifActivity(expression_type="Expression",
                                         expression_value="@pipeline().parameters.deltaload",
                                         name="deltaload eq true",
                                         description="True: delta load\nFalse: full load",
                                         if_false_activities=[
                                             copy_to_temp, sp_error_log1],
                                         if_true_activities=[
                                             lk_Watermarktable, lk_table, copy_to_temp_watermark, sp_WriteWatermark, sp_error_log2]
                                         )

        # following the if condition
        df_Temp_to_import = create_dfActivity(df_ref_name=f"DF_Import_ADLSTemp{tablename}",
                                              df_ref_type=dfreftype,
                                              name=f"DF_Import_ADLSTemp{tablename}",
                                              dataset_param=[df_ds_param],
                                              depends_on=[
                                                  depend_on(if_deltaload.name, succeeded)]
                                              )

        copy_Temp_to_Archiv = create_CopyActivity(name=f"ADLTemp{tablename}ToADLArchiv{tablename}",
                                                  sink=archiv_sink_type,  # parquet
                                                  source=archiv_source_type,
                                                  inputs_source=temp_dataset,
                                                  inputs_source_param=[
                                                      filename_param],
                                                  inputs_source_type=dsreftype,
                                                  outputs_sink=archiv_dataset,
                                                  outputs_sink_type=dsreftype,
                                                  outputs_sink_param=[
                                                      archive_filename_param],
                                                  depends_on=[
                                                      depend_on(df_Temp_to_import.name, succeeded)]
                                                  )

        sp_UpdateErrorLog = create_spActivity(linked_service_name=sql_linkedservice,
                                              linked_service_type=lsreftype,
                                              name="ASQL_prc_DFUpdateErrorTable",
                                              stored_procedure_name=error_sp,
                                              stored_procedure_parameters=create_sp_error_param(
                                                  df_Temp_to_import.name, "Temp", "Import"),
                                              depends_on=[
                                                  depend_on(df_Temp_to_import.name, failed)]
                                              )

        pipeline = create_pipeline(resource_name=f"PL_Import_DimASQL{tablename}",
                                   factory_name=factory_name_auto,
                                   resource_group_name=resource_name_auto,
                                   pipeline_name=f"PL_Import_DimASQL{tablename}",
                                   activities=[
                                       if_deltaload, df_Temp_to_import, copy_Temp_to_Archiv, sp_UpdateErrorLog],
                                   folder="Import",
                                   parameters=pipeline_params
                                   )

        pipeline_names.append({'pipeline_name': f"PL_Import_DimASQL{tablename}",
                               'pipeline_obj': pipeline
                            }) #f"PL_Import_DimASQL{tablename}"

    return pipeline_names


def create_custom_exe_activities(pipelines: list):
    df_pipelines =pd.DataFrame(pipelines)
    activities = []

    for i in range(len(df_pipelines)):
                    if i == 0:
                        print("first")
                        print(df_pipelines.loc[i,"pipeline_name"])
                        print("second")
                        print(df_pipelines.loc[i,"pipeline_obj"])
                        exe_PL = create_ExecutePipelineActivity(name=df_pipelines.loc[i,"pipeline_name"]+"_WoC",
                                                                pipeline_ref_name=df_pipelines.loc[i,"pipeline_obj"].name,
                                                                pipeline_ref_type=pipreftype,
                                                                wait_on_completion=True,
                                                                parameters=[Param("deltaload", "@pipeline().parameters.deltaload", "Bool")])
                        activities.append(exe_PL)
                    elif i >= 1:
                        print("first")
                        print(df_pipelines.loc[i,"pipeline_name"])
                        print("second")
                        print(df_pipelines.loc[i,"pipeline_obj"])
                        exe_PL = create_ExecutePipelineActivity(name=df_pipelines.loc[i,"pipeline_name"]+"_WoC",
                                                                pipeline_ref_name=df_pipelines.loc[i,"pipeline_obj"].name,
                                                                pipeline_ref_type=pipreftype,
                                                                wait_on_completion=True,
                                                                 parameters=[Param("deltaload", "@pipeline().parameters.deltaload", "Bool")],
                                                                depends_on=[depend_on(df_pipelines.loc[i-1,'pipeline_name']+"_WoC", succeeded)])
                        activities.append(exe_PL)
    return activities