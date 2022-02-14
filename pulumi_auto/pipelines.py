"""An Azure RM Python Pulumi program"""

from numpy import var
import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import datafactory


factory_name_auto = "htwcetdatafactory"
resource_name_auto = "pulumiauto"
dsreftype = "DatasetReference"
dfreftype = "DataFlowReference"
lsreftype = "LinkedServiceReference"

pipeline_email = azure_native.datafactory.Pipeline(resource_name="PL_Import_DimABLBEmail",
                                                   pipeline_name="PL_Import_DimABLBEmail",
                                                   factory_name=factory_name_auto,
                                                   resource_group_name=resource_name_auto,
                                                   folder=azure_native.datafactory.PipelineFolderArgs(
                                                       name="Import"
                                                   ),
                                                   activities=[
                                                       azure_native.datafactory.CopyActivityArgs(  # copy activity
                                                           name="ABLBEmailToADLTempEmail",
                                                           type="Copy",
                                                           source={
                                                               "type": "DelimitedTextSource",
                                                               "store_settings": {
                                                                   "type": "AzureBlobStorageReadSettings",
                                                                   "recursive": "true",
                                                                   "enablePartitionDiscovery": "false"
                                                               },
                                                               "format_settings": {
                                                                   "type": "DelimitedTextReadSettings"
                                                               }
                                                           },
                                                           sink={
                                                               "type": "ParquetSink",
                                                               "store_settings": {
                                                                   "type": "AzureBlobFSWriteSettings"
                                                               },
                                                               "format_settings": {
                                                                   "type": "ParquetWriteSettings"
                                                               }
                                                           },
                                                           translator={
                                                               "mappings": [
                                                                   {
                                                                       "source": {
                                                                           "name": "Login email",
                                                                           "type": "String",
                                                                           "physicalType": "String"
                                                                       },
                                                                       "sink": {
                                                                           "name": "Loginemail",
                                                                           "physicalType": "UTF8"
                                                                       }
                                                                   },
                                                                   {
                                                                       "source": {
                                                                           "name": "Identifier",
                                                                           "type": "String",
                                                                           "physicalType": "String"
                                                                       },
                                                                       "sink": {
                                                                           "name": "Identifier",
                                                                           "type": "String",
                                                                           "physicalType": "UTF8"
                                                                       }
                                                                   },
                                                                   {
                                                                       "source": {
                                                                           "name": "First name",
                                                                           "type": "String",
                                                                           "physicalType": "String"
                                                                       },
                                                                       "sink": {
                                                                           "name": "Firstname",
                                                                           "physicalType": "UTF8"
                                                                       }
                                                                   },
                                                                   {
                                                                       "source": {
                                                                           "name": "Last name",
                                                                           "type": "String",
                                                                           "physicalType": "String"
                                                                       },
                                                                       "sink": {
                                                                           "name": "Lastname",
                                                                           "physicalType": "UTF8"
                                                                       }
                                                                   }
                                                               ],
                                                               "typeConversion": "true",
                                                               "typeConversionSettings": {
                                                                   "allowDataTruncation": "true",
                                                                   "treatBooleanAsNumber": "false"
                                                               }
                                                           },
                                                           inputs=[
                                                               {
                                                                   "reference_name": "DS_ABLB_Email",
                                                                   "type": "DatasetReference"
                                                               }
                                                           ],
                                                           outputs=[
                                                               {
                                                                   "reference_name": "DS_ADLS_Temp",
                                                                   "type": "DatasetReference",
                                                                   "parameters": {
                                                                       "filename": {
                                                                           "value": "Email.parquet",
                                                                           "type": "Expression"
                                                                       }
                                                                   }
                                                               }
                                                           ]
                                                       ),
                                                       azure_native.datafactory.ExecuteDataFlowActivityArgs(  # dataflow activity
                                                           name="DF_Import_ADLTempEmail",
                                                           type="ExecuteDataFlow",
                                                           data_flow={
                                                               "reference_name": "DF_Import_ADLSTempEmail",
                                                               "type": "DataFlowReference",
                                                               "dataset_parameters": {
                                                                   "SRCADLTempEmail": {
                                                                       "filename": "Email.parquet"
                                                                   }
                                                               }
                                                           },
                                                           depends_on=[
                                                               {
                                                                   "activity": "ABLBEmailToADLTempEmail",
                                                                   "dependency_conditions": [
                                                                       "Succeeded"
                                                                   ]
                                                               }
                                                           ]
                                                       ),
                                                       azure_native.datafactory.SqlServerStoredProcedureActivityArgs(  # sp activity
                                                           name="ASQL_prc_CDUpdateErrorTable",
                                                           type="SqlServerStoredProcedure",
                                                           stored_procedure_name="[dbo].[UpdateErrorTable]",
                                                           linked_service_name={
                                                               "reference_name": "LS_ASQL_SalesLT",
                                                               "type": "LinkedServiceReference"
                                                           },
                                                           depends_on=[
                                                               {
                                                                   "activity": "ABLBEmailToADLTempEmail",
                                                                   "dependency_conditions": [
                                                                       "Failed"
                                                                   ]
                                                               }
                                                           ],
                                                           stored_procedure_parameters={
                                                               "DataFactory_Name": {
                                                                   "value": {
                                                                       "value": "@pipeline().DataFactory",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "Destination": {
                                                                   "value": "temp",
                                                                   "type": "String"
                                                               },
                                                               "ErrorCode": {
                                                                   "value": {
                                                                       "value": "@{activity('ABLBEmailToADLTempEmail').error.errorCode}",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "ErrorDescription": {
                                                                   "value": {
                                                                       "value": "@{activity('ABLBEmailToADLTempEmail').error.message}",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "ErrorLoggedTime": {
                                                                   "value": {
                                                                       "value": "@utcnow()",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "Execution_Status": {
                                                                   "value": {
                                                                       "value": "@{activity('ABLBEmailToADLTempEmail').output.executionDetails[0].status}",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "FailureType": {
                                                                   "value": {
                                                                       "value": "@concat(activity('ABLBEmailToADLTempEmail').error.message,'failureType:',activity('ABLBEmailToADLTempEmail').error.failureType)\n",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "Pipeline_Name": {
                                                                   "value": {
                                                                       "value": "@pipeline().Pipeline",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "RunId": {
                                                                   "value": {
                                                                       "value": "@pipeline().RunId",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "Sink_Type": {
                                                                   "value": {
                                                                       "value": "@{activity('ABLBEmailToADLTempEmail').output.executionDetails[0].sink.type}",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "Source": {
                                                                   "value": "cethtwsource2/source",
                                                                   "type": "String"
                                                               },
                                                               "Source_Type": {
                                                                   "value": {
                                                                       "value": "@{activity('ABLBEmailToADLTempEmail').output.executionDetails[0].source.type}\n",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               }
                                                           }
                                                       ),
                                                       azure_native.datafactory.SqlServerStoredProcedureActivityArgs(  # sp activity
                                                           name="ASQL_prc_DFUpdateErrorTable",
                                                           type="SqlServerStoredProcedure",
                                                           stored_procedure_name="[dbo].[UpdateErrorTable]",
                                                           linked_service_name={
                                                               "reference_name": "LS_ASQL_SalesLT",
                                                               "type": "LinkedServiceReference"
                                                           },
                                                           depends_on=[
                                                               {
                                                                   "activity": "DF_Import_ADLTempEmail",
                                                                   "dependency_conditions": [
                                                                       "Failed"
                                                                   ]
                                                               }
                                                           ],
                                                           stored_procedure_parameters={
                                                               "DataFactory_Name": {
                                                                   "value": {
                                                                       "value": "@pipeline().DataFactory",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "Destination": {
                                                                   "value": "import",
                                                                   "type": "String"
                                                               },
                                                               "ErrorCode": {
                                                                   "value": {
                                                                       "value": "@activity('DF_Import_ADLTempEmail').output.error[0].Code",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "ErrorDescription": {
                                                                   "value": {
                                                                       "value": "@activity('DF_Import_ADLTempEmail').output.error[0].Message",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "ErrorLoggedTime": {
                                                                   "value": {
                                                                       "value": "@utcnow()",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "Execution_Status": {
                                                                   "value": "k.a.",
                                                                   "type": "String"
                                                               },
                                                               "FailureType": {
                                                                   "value": "k.a.",
                                                                   "type": "String"
                                                               },
                                                               "Pipeline_Name": {
                                                                   "value": {
                                                                       "value": "@pipeline().Pipeline",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "RunId": {
                                                                   "value": {
                                                                       "value": "@pipeline().RunId",
                                                                       "type": "Expression"
                                                                   },
                                                                   "type": "String"
                                                               },
                                                               "Sink_Type": {
                                                                   "value": "k.a.",
                                                                   "type": "String"
                                                               },
                                                               "Source": {
                                                                   "value": "temp",
                                                                   "type": "String"
                                                               },
                                                               "Source_Type": {
                                                                   "value": "k.a.",
                                                                   "type": "String"
                                                               }
                                                           }
                                                       ),
                                                       azure_native.datafactory.CopyActivityArgs(  # copy activity
                                                           name="ABLBTempEmailToADLArchivEmail",
                                                           type="Copy",
                                                           depends_on=[
                                                               {
                                                                   "activity": "DF_Import_ADLTempEmail",
                                                                   "dependency_conditions": [
                                                                       "Succeeded"
                                                                   ]
                                                               }
                                                           ],
                                                           source={
                                                               "type": "ParquetSource",
                                                               "store_settings": {
                                                                   "type": "AzureBlobFSReadSettings",
                                                                   "recursive": "true",
                                                                   "enablePartitionDiscovery": "false"
                                                               }
                                                           },
                                                           sink={
                                                               "type": "ParquetSink",
                                                               "store_settings": {
                                                                   "type": "AzureBlobFSWriteSettings"
                                                               },
                                                               "format_settings": {
                                                                   "type": "ParquetWriteSettings"
                                                               }
                                                           },
                                                           translator={
                                                               "mappings": [
                                                                   {
                                                                       "source": {
                                                                           "name": "Loginemail",
                                                                           "type": "String",
                                                                           "physicalType": "UTF-8"
                                                                       },
                                                                       "sink": {
                                                                           "name": "Loginemail",
                                                                           "type": "String",
                                                                           "physicalType": "UTF-8"
                                                                       }
                                                                   },
                                                                   {
                                                                       "source": {
                                                                           "name": "Identifier",
                                                                           "type": "String",
                                                                           "physicalType": "UTF-8"
                                                                       },
                                                                       "sink": {
                                                                           "name": "Identifier",
                                                                           "type": "String",
                                                                           "physicalType": "UTF8"
                                                                       }
                                                                   },
                                                                   {
                                                                       "source": {
                                                                           "name": "Firstname",
                                                                           "type": "String",
                                                                           "physicalType": "UTF-8"
                                                                       },
                                                                       "sink": {
                                                                           "name": "Firstname",
                                                                           "type": "String",
                                                                           "physicalType": "UTF-8"
                                                                       }
                                                                   },
                                                                   {
                                                                       "source": {
                                                                           "name": "Lastname",
                                                                           "type": "String",
                                                                           "physicalType": "UTF-8"
                                                                       },
                                                                       "sink": {
                                                                           "name": "Lastname",
                                                                           "type": "String",
                                                                           "physicalType": "UTF-8"
                                                                       }
                                                                   }
                                                               ],
                                                               "typeConversion": "true",
                                                               "typeConversionSettings": {
                                                                   "allowDataTruncation": "true",
                                                                   "treatBooleanAsNumber": "false"
                                                               }
                                                           },
                                                           inputs=[
                                                               {
                                                                   "reference_name": "DS_ADLS_Temp",
                                                                   "type": "DatasetReference",
                                                                   "parameters": {
                                                                       "filename": {
                                                                           "value": "Email.parquet",
                                                                           "type": "Expression"
                                                                       }
                                                                   }
                                                               }
                                                           ],
                                                           outputs=[
                                                               {
                                                                   "reference_name": "DS_ADLS_Archiv",
                                                                   "type": "DatasetReference",
                                                                   "parameters": {
                                                                       "filename": {
                                                                           "value": "@concat('Email_',utcnow(),'.parquet')",
                                                                           "type": "Expression"
                                                                       }
                                                                   }
                                                               }
                                                           ]
                                                       )
                                                   ]
                                                   )


pipeline_master = azure_native.datafactory.Pipeline(resource_name="PL_Import_Master",
                                                    pipeline_name="PL_Import_Master",
                                                    factory_name=factory_name_auto,
                                                    resource_group_name=resource_name_auto,
                                                    activities=[
                                                        azure_native.datafactory.ExecutePipelineActivityArgs(
                                                            name="PL_Import_DimABLBEmail_WoC",
                                                            type="ExecutePipeline",
                                                            pipeline={
                                                                "reference_name": "PL_Import_DimABLBEmail",
                                                                "type": "PipelineReference"
                                                            },
                                                            wait_on_completion=True,
                                                        )
                                                    ],
                                                    parameters={
                                                        "deltaload": {
                                                            "type": "Bool"
                                                        }
                                                    },
                                                    folder=azure_native.datafactory.PipelineFolderArgs(
                                                        name="Master"
                                                    )
                                                    )


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

class Var:
    
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

#brauchen wir alle params? ok
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
    pass


def dbsource():
    pass


def sinks(sink_type: str):
    switch = {
        "parquet": parquetsink()
    }
    return switch.get(sink_type, "Sink type should be: parquet, ...")


def sources(source_type: str):
    switch = {
        "parquet": parquetsource(),
        "delimitedtext": delimitedsource(),
        "db": dbsource()
    }
    return switch.get(source_type, "Source type should be: parquet, delimetedtext, db ...")

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
                        sink: str,
                        source: str,
                        data_integration_units: int = None,
                        depends_on: list = None,
                        enable_skip_incompatible_row: bool = False,
                        enable_staging: bool = False,
                        inputs_source: str = None,
                        inputs_source_param: list = None,
                        inputs_source_type: str = None,
                        linked_service_name: str = None,
                        linked_service_type: str = None,
                        linked_service_param: list = None,
                        outputs_sink: str = None,
                        outputs_sink_param: list = None,
                        outputs_sink_type: str = None,
                        parallel_copies: int = None,
                        preserve: list = None,
                        preserve_rules: list = None,
                        redirect_incompatible_row_settings = None,
                        translator: list = None,
                        skip_error_file = None,
                        validate_data_consistency: bool = False):

    activity = datafactory.CopyActivityArgs(
        name=name,
        type="Copy",
        sink=sinks(sink),
        source=sources(source),
        data_integration_units=data_integration_units,
        depends_on=depends_on,
        enable_skip_incompatible_row=enable_skip_incompatible_row,
        enable_staging=enable_staging,
        inputs=dataset_ref(inputs_source, inputs_source_type,
                           param(inputs_source_param)),
        linked_service_name=linked_service_reference(
            linked_service_name, linked_service_type, param(linked_service_param)),
        outputs=dataset_ref(outputs_sink, outputs_sink_type,
                            param(outputs_sink_param)),
        parallel_copies=parallel_copies,
        preserve=preserve,
        preserve_rules=preserve_rules,
        redirect_incompatible_row_settings=redirect_incompatible_row_settings,
        skip_error_file=skip_error_file,
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
            linked_service_name, linked_service_type, param(linked_service_param)),
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
        data_flow=dataflow_ref(df_ref_name, df_ref_type,df_ds_param(dataset_param), param(df_param)),
        name=name,
        compute=compute(compute_type, compute_core_count),
        continue_on_error=continue_on_error,
        depends_on=depends_on,
        linked_service_name=linked_service_reference(linked_service_name, linked_service_type, param(linked_service_param)),
        run_concurrently=run_concurrently,
        staging=staging(staging_folder_path, linked_service_reference(staging_linked_service_name, staging_linked_service_type, param(staging_linked_service_param))),
        trace_level=trace_level #‘coarse’, ‘fine’, and ‘none’
    )
    return activity


def create_lkActivity(ds_ref_name: str,
                      ds_ref_type: str,
                      name: str,
                      source: str, 
                      ds_ref_param: list = None,
                      depends_on: list = None,
                      first_row_only: bool = True,
                      linked_service_name: str = None,
                      linked_service_type: str = None,
                      linked_service_param: list = None
                      ):
    
    activity = datafactory.LookupActivityArgs(
        dataset=dataset_ref(ds_ref_name, ds_ref_type,param(ds_ref_param)),
        name=name,
        source=sources(source),
        depends_on=depends_on,
        first_row_only=first_row_only,
        linked_service_name=linked_service_reference(linked_service_name, linked_service_type, param(linked_service_param))
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


# test
test_copy = datafactory.CopyActivityArgs(  # copy activity
    name="ABLBTempEmailToADLArchivEmailTest",
    type="Copy",
    sink=datafactory.ParquetSinkArgs(
        type="ParquetSink",
        store_settings=datafactory.AzureBlobFSWriteSettingsArgs(
            type="AzureBlobFSWriteSettings")),
    source=datafactory.ParquetSourceArgs(
        type="ParquetSource",
        store_settings=datafactory.AzureBlobFSReadSettingsArgs(
            type="AzureBlobFSReadSettings"
        )
    ))

test_copy2 = datafactory.CopyActivityArgs(  # copy activity
    name="ABLBTempEmailToADLArchivEmailTest2",
    type="Copy",
    sink=datafactory.ParquetSinkArgs(
        type="ParquetSink",
        store_settings=datafactory.AzureBlobFSWriteSettingsArgs(
            type="AzureBlobFSWriteSettings")),
    source=datafactory.ParquetSourceArgs(
        type="ParquetSource",
        store_settings=datafactory.AzureBlobFSReadSettingsArgs(
            type="AzureBlobFSReadSettings"
        )
    ),
    depends_on=[datafactory.ActivityDependencyArgs(
        activity="ABLBTempEmailToADLArchivEmailTest",
        dependency_conditions=["Succeeded"]
    )])


test3 = create_CopyActivity(name="ABLBTempEmailToADLArchivEmailTest3",
                            sink="parquet",
                            source="parquet",
                            )

test4 = create_CopyActivity(name="ABLBTempEmailToADLArchivEmailTest4",
                            sink="parquet",
                            source="parquet",
                            inputs_source="DS_ADLS_Temp",
                            inputs_source_type="DatasetReference"
                            )

acitivity_list = [test_copy, test_copy2, test3, test4]
for i in range(1, 10):
    create_pipeline(f"PL_Import_Master_test_{i}", factory_name_auto,
                    resource_name_auto, f"PL_Import_Master_test_{i}", folder="Test", activities=acitivity_list)


exe = create_ExecutePipelineActivity(
    "exe", "PL_Import_Master_test_1", "PipelineReference")
exe2 = create_ExecutePipelineActivity("exe2", "PL_Import_Master_test_2",
                                      "PipelineReference", depends_on=[depend_on(exe.name, ["Failed"])])
exe3 = create_spActivity("LS_ASQL_SalesLT", lsreftype,
                         "exe3", "[dbo].[UpdateErrorTable]")
create_pipeline("test_exe", factory_name_auto, resource_name_auto,
                "test_exe", activities=[exe, exe2, exe3], folder="Test")
