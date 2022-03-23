"""An Azure RM Python Pulumi program"""

import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import datafactory
import htw_config as cfg
import pandas as pd

# https://docs.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-2017&tabs=ssms adventure works sample databases
# Pipeline
# API Reference: https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/pipeline/

# reference types, specified for dataset, dataflow, pipeline and linked service reference
dsreftype = "DatasetReference"
dfreftype = "DataFlowReference"
lsreftype = "LinkedServiceReference"
pipreftype = "PipelineReference"

# use for dependencies between activities
succeeded = ["Succeeded"]
failed = ["Failed"]
suc_fail = ["Succeeded", "Failed"]


# class

class Mapping:
    """ Mapping Class, if mapping of attributes between source and sink dataset is required"""
    def __init__(self,
                 source_name: str,
                 source_type: str,
                 source_physical_type: str,
                 sink_name: str,
                 sink_type: str,
                 sink_physical_type: str):
        """
        Note: Attribute type and physical type can be the same
        
        Args:
            source_name (str): data source attribute name
            source_type (str): attribute type
            source_physical_type (str): attribute physical type
            sink_name (str): data sink attribute name
            sink_type (str): attribute type
            sink_physical_type (str): attribute physical type
        """
        self.source_name = source_name
        self.source_type = source_type
        self.source_physical_type = source_physical_type
        self.sink_name = sink_name
        self.sink_type = sink_type
        self.sink_physical_type = sink_physical_type


class Param:
    """Class to represent the following structure of param in azure data factory:
        
        e.g. filename param
        {"filename": 
                    { "value": "Email.parquet",
                    "type": "Expression"}
    """
    def __init__(self, name_: str, value_: str, type_: str):
        """
        Param has the following values
        Args:
            name_ (str): param name
            value_ (str): param value
            type_ (str): param type
        """
        self.name = name_
        self.value = value_
        self.type = type_


class Variable:
    """Class to represent a var in azure data factory. As is has the same structure as a param but can only
     be the type of String, Boolean and Array
    """
    def __init__(self, name: str, type_: str, default_: str) -> None:
        """Variable has the following values

        Args:
            name (str): var name
            type_ (str): var type
            default_ (str): var default value

        Raises:
            ValueError: if not string, bool or array
        """
        if type not in ["String", "Boolean", "Array"]:
            raise ValueError("Possible Types: String, Boolean, Array")
        self.name = name
        self.type = type_
        self.default = default_


class DataflowDatasetParam:
    """Class to represent a dataset param when listed in a Dataflow as is follows the following
    convention, e.g.
    
    "dataset_parameters": {
            "SRCADLTempEmail": {
                            "filename": "Email.parquet"
                            }
                        }    
    """
    def __init__(self, dataflow_activity_name: str, param_names: list, param_values: list):
        """
        This is a special type of param and will be treated different than param.
        Note: As a dataset can have multiple parameters, param_names and param_values are list.
        Args:
            dataflow_activity_name (str): dataflow name
            param_names (list): names of the param
            param_values (list): values

        Raises:
            ValueError: if the length of param_names and param_values mismatch. They have to be the same length
            for the matching of values and names.
        """
        if len(param_names) != len(param_values):
            raise ValueError("Length mismatch of Parameter names and values.")
        self.dataflow_activity_name = dataflow_activity_name
        self.param_names = param_names
        self.param_values = param_values


# help methods for the activities

# [Mapping()]
def mapping(pairs: list):
    """Input a list of Mapping objects and return a dict for the data factory frontend to read.
        It has to be returned as mappings: [source: {}, sink: {} etc.]
        
    Args:
        pairs (list): list of Mapping objects

    Returns:
        _type_: dict of mapped values if pairs is specified, otherwise None
    """
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

#not used
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
    """Create a param specific for stored parameters

    Args:
        params (list): input a list of Param() objects

    Returns:
        _type_: dictionary of params if specified, otherwise none
    """
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
    """Input a list of param to get the dict representation for data factory to read from
        
    Args:
        params (list): list of dict
        flag (int, optional): indicates different structure for copy and lookup activity. Defaults to None.

    Returns:
        _type_: dictionary of params, otherwise none
    """
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
    """Input a list of Variables and get the dict representation for data factory to read from

    Args:
        vars (list): list of Variable objects

    Returns:
        _type_: dictionary representations of the objects
    """
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
    """Input a list of Parameters and get the dict representation for data factory to read from.
    This representation is unique for dataset params in a dataflow activity
    
    e.g.
    "SRCADLTempEmail": {
                     "filename": "Email.parquet"
                    "some_other": "dummy"
                    etc.
                    }

    Args:
        params (list): input list of DataflowDatasetParam objects

    Returns:
        _type_: dictionary if specified, otherwise none
    """
    dict = {}
    if not params:
        return None

    for param in params:
        for names, values in zip(param.param_names, param.param_values):
            if param.dataflow_activity_name not in dict.keys(): # check if dataflow_activity_name is already in dict
                dict[param.dataflow_activity_name] = {
                    names: values
                }
            elif param.dataflow_activity_name in dict.keys(): # if already in dict, just add value to the key, which is the dataflow_activity_name 
                dict[param.dataflow_activity_name][names] = values
    return dict

# logerror


def create_sp_error_param(activity: str, source: str, sink: str):
    """This create the params for our stored procedure error log

    Args:
        activity (str): name of the activity to log
        source (str): name of source to log
        sink (str): name of sink to log

    Returns:
        _type_: list of Param objects
    """
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
    """Create a dataset reference for an activity 

    Args:
        reference_name (str): Name of the dataset which is reference to
        type_ (str): static value - "DatasetReference"
        parameters (list, optional): list of Param objects e.g. {param_name: {value, type}}. Defaults to None.
        flag (int, optional): flag 0: Copyactivity 1: lookupactivity. Defaults to None.

    Returns:
        _type_: DatasetReference (Pulumi type)
    """
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
    """Create a dataflow reference for an activity

    Args:
        reference_name (str): name of the dataflow which is reference to
        type_ (str): static value - "DataFlowReference"
        dataset_parameters (list, optional): list of DataflowDatasetParam objects. Defaults to None.
        parameters (list, optional): param of the dataflow. Defaults to None.

    Returns:
        _type_: DataFlowReference (pulumi type)
    """
    ref = datafactory.DataFlowReferenceArgs(
        reference_name=reference_name,
        type=type_,
        dataset_parameters=df_ds_param(dataset_parameters),
        parameters=param(parameters)
    )
    return ref


# type : LinkedServiceReference
def linked_service_reference(reference_name: str, type_: str, parameters: list = None):
    """Create a linked service reference

    Args:
        reference_name (str): name of the linked service
        type_ (str): static value - "LinkedServiceReference"
        parameters (list, optional): list of Param objects. Defaults to None.

    Returns:
        _type_: LinkedService (pulumi type)
    """
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
    """Create dependency between activities
    
    Args:
        activity (str): depend on activity
        cond (list): condition of the dependency: success, failed or both

    Returns:
        _type_: ActivityDependency (pulumi type)
    """
    if (not activity and not cond):
        return None

    dep = datafactory.ActivityDependencyArgs(
        activity=activity,
        dependency_conditions=cond
    )
    return dep


def pipeline_ref(pipeline_name: str, pipeline_type: str, ref_name: str = None):
    """create a pipeline reference (used for the execute pipeline activity)

    Args:
        pipeline_name (str): name of the referenced pipeline
        pipeline_type (str): type of the pipeline reference e.g. PipelineReference
        ref_name (str, optional): internal ref name used by pulumi. Defaults to None.

    Returns:
        _type_: _description_
    """
    pipeline_ref = datafactory.PipelineReferenceArgs(
        reference_name=pipeline_name,
        type=pipeline_type,
        name=ref_name
    )
    return pipeline_ref


# Only standard params for source and sink
def parquetsink():
    """create a parquet type sink

    Returns:
        _type_: ParquetSink (pulumi type)
    """
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
    """create a parquet type source

    Returns:
        _type_: ParquetSource (pulumi type)
    """
    source = datafactory.ParquetSourceArgs(
        type="ParquetSource",
        store_settings=datafactory.AzureBlobFSReadSettingsArgs(
            type="AzureBlobFSReadSettings"
        ),
        disable_metrics_collection=True
    )
    return source


def delimitedsource():
    """create a delimited type source

    Returns:
        _type_: DelimitedTextSource (pulumi type)
    """
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
    """create a azuresql type source

    Args:
        query (str): query for the azuresql source

    Returns:
        _type_: AzureSqlSource (pulumi type) 
    """
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
    """switch between sink type, can be extend with additional types

    Args:
        sink_type (str): just "parquet" for now

    Returns:
        _type_: Warning if sink type is not parquet (can be extended)
    """
    switch = {
        "parquet": parquetsink()
    }
    return switch.get(sink_type, "Sink type should be: parquet, ...")


def sources(source_type: str, query: str = None):
    """switch between sources parquet, delimitedtext, azuresql

    Args:
        source_type (str): either parquet, delimitedtext or azuresql (can be extended)
        query (str, optional): query for the sql source. Defaults to None.

    Returns:
        _type_: _description_
    """
    switch = {
        "parquet": parquetsource(),
        "delimitedtext": delimitedsource(),
        "azuresql": dbsource(query)
    }
    return switch.get(source_type, "Source type should be: parquet, delimetedtext or azuresql.")


# compute_type = [‘General’, ‘MemoryOptimized’, ‘ComputeOptimized’]
# core_count = 8, 16, 32, 48, 80, 144 and 272
def compute(compute_type: str, core_count: int):
    """create the compute type for the execute dataflow activity

    Args:
        compute_type (str): can be [General, MemoryOptimized, ComputeOptimized]
        core_count (int): can be 8, 16, 32, 48, 80, 144 and 272

    Returns:
        _type_: ExecuteDataFlowActivityTypePropertiesResponseCompute (pulumi type), none if not specified
    """
    if (not compute_type and not core_count):
        return None

    c = datafactory.ExecuteDataFlowActivityTypePropertiesResponseComputeArgs(
        compute_type=compute_type,
        core_count=core_count
    )
    return c


def staging(folder_path: str, linked_service: datafactory.LinkedServiceReferenceArgs):
    """create data flow staging parameter

    Args:
        folder_path (str): folder for the staging
        linked_service (datafactory.LinkedServiceReferenceArgs): return of linked_service_reference()

    Returns:
        _type_: DataFlowStagingInfo (pulumi type)
    """
    if (not folder_path and not linked_service):
        return None

    s = datafactory.DataFlowStagingInfoArgs(
        folder_path=folder_path,
        linked_service=linked_service
    )
    return s


def foldername(foldername: str):
    """create folder for data pipelines

    Args:
        foldername (str): name of the folder

    Returns:
        _type_: PipelineFolder (pulumi type)
    """
    if not foldername:
        return None

    f = datafactory.PipelineFolderArgs(name=foldername)
    return f


def redirect_rows(linked_service_name: str, path: str):
    """create the redirect incompatible rows parameter

    Args:
        linked_service_name (str): name of the linkedservice
        path (str): path to redirect rows to

    Returns:
        _type_: RedirectIncompatibleRowSettings (pulumi type)
    """
    if not linked_service_name and not path:
        return None

    settings = datafactory.RedirectIncompatibleRowSettingsArgs(
        linked_service_name=linked_service_name,
        path=path
    )
    return settings


def skip_error_file(data_inconsistency: bool, file_missing: bool):
    """create skip error file parameter

    Args:
        data_inconsistency (bool): 
        file_missing (bool): _description_

    Returns:
        _type_: _description_
    """
    settings = datafactory.SkipErrorFileArgs(
        data_inconsistency=data_inconsistency,
        file_missing=file_missing
    )
    return settings


def if_expression(type_: str, value: str):
    """create the expression for if activity

    Args:
        type_ (str): type of the expression e.g. bool
        value (str): value of the expression e.g. true

    Returns:
        _type_: Expression (pulumi type)
    """
    ex = datafactory.ExpressionArgs(
        type=type_,
        value=value
    )
    return ex


def create_filename_param(value: str):
    """set filename param for our custom pipeline

    Args:
        value (str): value for filename param

    Returns:
        _type_: filename param
    """
    filename = Param("filename", f"{value}.parquet", "Expression")
    return filename


def create_archiv_filename_param(value: str):
    """set filename of the archive file

    Args:
        value (str): value for the param

    Returns:
        _type_: filename param
    """
    filename = Param(
        "filename", f"@concat('{value}_',utcnow(),'.parquet')", "Expression")
    return filename


def create_df_ds_param(tablename: str, param_names: list, param_values: list):
    """create the dataset param of the dataflow

    Args:
        tablename (str): name of the sql table/ csv file
        param_names (list): list of parameter names
        param_values (list): list of the corresponding values

    Returns:
        _type_: DataflowDatasetParam
    """
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
    """create a execute pipeline activity, which act as a parent for the specified pipeline

    Args:
        name (str): name of the activity, required args
        pipeline_ref_name (str): name of the pipeline reference to, required args
        pipeline_ref_type (str): type of the reference e.g. PipelineReference, required args
        depends_on (list, optional): list of pipeline dependencies e.g. [depend_on(), ...]. Defaults to None.
        description (str, optional): pipeline description. Defaults to None.
        parameters (list, optional): parameters for this pipeline. Defaults to None.
        wait_on_completion (bool, optional): wait for the activity to complete before moving on. Defaults to None.

    Returns:
        _type_: ExecutePipelineActivity (pulumi type)
    """
    activity = datafactory.ExecutePipelineActivityArgs(
        name=name[:55],
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
                        linked_service_name: str = None,
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
    """Create a copy activity, which copy data from source to sink.
        Parameter sink and source specify the type, whereas inputs_x and outputs_x define values for source and sink. 
    Args:
        name (str): name of the copy activity, max 55 char length, required args
        sink (str): specify the sink type e.g. parquet, required args
        source (str): specify the source type e.g. parquet, delimitedtext, azuresql, required args
        sql_query (str): sql query for azuresql source. Defaults to None.
        depends_on (list, optional): dependency of the activity e.g. [depend_on(), ...]. Defaults to None.
        enable_skip_incompatible_row (bool, optional): skip incompatible row if set to true. Defaults to False.
        enable_staging (bool, optional): enable staging for error handling. Defaults to False.
        inputs_source (str, optional): name of the source dataset. Defaults to None.
        inputs_source_param (list, optional): params of the source dataset. Defaults to None.
        inputs_source_type (str, optional): type of the reference to source dataset e.g. DatasetReference. Defaults to None.
        linked_service_name (str, optional): linked service name. Defaults to None.
        linked_service_type (str, optional): linked service type. Defaults to None.
        linked_service_param (list, optional): params of the linked service. Defaults to None.
        outputs_sink (str, optional): name of the sink dataset. Defaults to None.
        outputs_sink_param (str, optional): params of the sink dataset. Defaults to None.
        outputs_sink_type (str, optional): type of the reference e.g. DatasetReference. Defaults to None.
        parallel_copies (int, optional): Maximum number of concurrent sessions opened on the source or sink to avoid overloading the data store. Defaults to None.
        preserve (list, optional): preserve rules. Defaults to None.
        redirect_incompatible_row_settings_path (str, optional): Redirect incompatible row settings when EnableSkipIncompatibleRow is true. Defaults to None.
        translator (list, optional): Copy activity translator for mapping data (expect list of mapping objects). If not specified, tabular translator is used. Defaults to None.
        skip_error_file_file_missing (bool, optional): Specify the fault tolerance for data consistency. Defaults to False.
        validate_data_consistency (bool, optional):Whether to enable Data Consistency validation. Defaults to False.

    Returns:
        _type_: CopyActivity (pulumi type)
    """
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
    """create a stored procedure activity

    Args:
        linked_service_name (str): name of the linked service, required args
        linked_service_type (str): type of the linked service e.g. LinkedServiceReference, required args
        name (str): name of the activity
        stored_procedure_name (str): name of the stored procedure
        linked_service_param (list, optional): params of the linked service. Defaults to None.
        depends_on (list, optional): dependencies of the activity. Defaults to None.
        stored_procedure_parameters (list, optional): parameter of the stored procedure. Defaults to None.

    Returns:
        _type_: SqlServerStoredProcedureActivity (pulumi type)
    """
    activity = datafactory.SqlServerStoredProcedureActivityArgs(
        linked_service_name=linked_service_reference(
            linked_service_name, linked_service_type, linked_service_param),
        name=name[:55],
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
    """create an if activity

    Args:
        expression_type (str): the type of the expression to check for e.g. bool, required
        expression_value (str): the value which should be checked by the if activity, required
        name (str): name of the activity, required
        depends_on (list, optional): dependencies for the activity. Defaults to None.
        description (str, optional): description of the activity. Defaults to None.
        if_false_activities (list, optional): store activities if expression is false. Defaults to None.
        if_true_activities (list, optional): store activities if expression is true. Defaults to None.

    Returns:
        _type_: IfConditionActivity (pulumi type)
    """
    activity = datafactory.IfConditionActivityArgs(
        expression=if_expression(expression_type, expression_value),
        name=name[:55],
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
    """create a dataflow activity

    Args:
        df_ref_name (str): name if the reference dataflow, required 
        df_ref_type (str): type of the reference e.g. DataFlowReference, required
        name (str): name of the activity, required
        dataset_param (list, optional): param of the dataset, which the dataflow used. Defaults to None.
        df_param (list, optional): param of the dataflow. Defaults to None.
        compute_type (str, optional): Compute properties for data flow activity. Defaults to None.
        compute_core_count (int, optional): Compute properties for data flow activity. Defaults to None.
        continue_on_error (bool, optional): Continue on error setting used for data flow execution. Enables processing to continue if a sink fails. Defaults to None.
        depends_on (list, optional): Activity depends on condition. Defaults to None.
        linked_service_name (str, optional): linked service reference name . Defaults to None.
        linked_service_type (str, optional): type of the reference e.g. LinkedServiceReference. Defaults to None.
        linked_service_param (dict, optional): param of the linked service. Defaults to None.
        run_concurrently (bool, optional): Concurrent run setting used for data flow execution. Allows sinks with the same save order to be processed concurrently. Defaults to None.
        staging_folder_path (str, optional): path if for staging. Defaults to None.
        staging_linked_service_name (str, optional): linked service name for staging. Defaults to None.
        staging_linked_service_type (str, optional): type of the reference. Defaults to None.
        staging_linked_service_param (list, optional): param of the linkedservice. Defaults to None.
        trace_level (str, optional): Trace level setting used for data flow monitoring output. Supported values are: coarse, fine, and none. Defaults to None.

    Returns:
        _type_: dataflow activity
    """
    activity = datafactory.ExecuteDataFlowActivityArgs(
        data_flow=dataflow_ref(df_ref_name, df_ref_type,
                               dataset_param, df_param),
        type="ExecuteDataFlow",
        name=name[:55],
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
    """create a lookup activity

    Args:
        ds_ref_name (str): name of the dataset which is referenced to, required 
        ds_ref_type (str): type of the reference name e.g. DatasetReference, required
        name (str): name of the activity, required
        source (str): source type for the lookup activity e.g. azuresql, required
        sql_query (str, optional): sql query for the lookup activity. Defaults to None.
        ds_ref_param (list, optional): param of the dataset which is referenced to. Defaults to None.
        depends_on (list, optional): dependencies for the activity. Defaults to None.
        first_row_only (bool, optional): Whether to return first row or all rows. Default value is true. Defaults to True.
        linked_service_name (str, optional): name of the linked service reference. Defaults to None.
        linked_service_type (str, optional): type of the linked service e.g. "LinkedServiceReference". Defaults to None.
        linked_service_param (list, optional): param of the linked service. Defaults to None.

    Returns:
        _type_: LookupActivity (pulumi type)
    """
    activity = datafactory.LookupActivityArgs(
        dataset=dataset_ref(ds_ref_name, ds_ref_type, ds_ref_param, 1),
        name=name[:55],
        type="Lookup",
        source=sources(source, sql_query),
        depends_on=depends_on,
        first_row_only=first_row_only,
        linked_service_name=linked_service_reference(
            linked_service_name, linked_service_type, linked_service_param)
    )
    return activity


def create_pipeline(resource_name: str,
                    factory_name: str,
                    resource_group_name: str,
                    pipeline_name: str = None,
                    activities: list = None,
                    concurrency: int = None,
                    folder: str = None,
                    parameters: list = None,
                    variables: list = None):
    """create a pipeline activity

    Args:
        resource_name (str): internal resource name for pulumi, required
        factory_name (str): factory name, required
        resource_group_name (str): resource group name, required
        pipeline_name (str, optional): name of the pipeline. Defaults to None.
        activities (list, optional): list of activities for the pipeline. Defaults to None.
        concurrency (int, optional): The max number of concurrent runs for the pipeline. Defaults to None.
        folder (str, optional): The folder that this Pipeline is in. If not specified, Pipeline will appear at the root level. Defaults to None.
        parameters (list, optional): List of parameters for pipeline. Defaults to None.
        variables (list, optional): List of variables for pipeline. Defaults to None.

    Returns:
        _type_: Pipeline (pulumi up)
    """
    pipeline = datafactory.Pipeline(resource_name=resource_name,
                                    opts = pulumi.ResourceOptions(delete_before_replace=True),  
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
    """Create our custom pipelines according to the template from ceteris for csv files.
        Activities: Copy, DataFlow, StoredProcedure, Pipeline
        Parameter: filename, archiv_filename

    Args:
        csv (str): name of the csv table
        csv_dataset (str): dataset of the csv
        csv_dataset_sink_type (str): csv dataset sink type
        csv_dataset_source_type (str): csv dataset source type
        sql_linked_service (str): linked service name for stored procedure activity
        error_sp (str): name of the error log stored procedure
        archiv_dataset (str): archive dataset name
        temp_dataset (str): temp dataset name
        archiv_source_type (str): archiv source type
        archiv_sink_type (str): archive sink type
        resource_name_auto (_type_): resource  group name
        factory_name_auto (_type_): factory name

    Returns:
        _type_: Pipelines for csv files according to the given template in a dictionary with according names. The Pipeline objects are needed for pulumi to wait
                for the creation of one resources before creating another one.
    """
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
                                       archiv_source_type: str, # don't need source typ as is it the same in our case
                                       archiv_sink_type: str,
                                       pipeline_params: list,
                                       factory_name_auto,
                                       resource_name_auto):
    """Create our sql source pipelines according to the given template of ceteris.
        Activities: Copy, If, DataFlow, StoredProcedure, Lookup
        Parameters: filename, archiv_filename
        
    Args:
        tablenames (list): list of sql table names
        sql_dataset (str): sql dataset 
        sql_dataset_sink_type (str): sql dataset sink type
        sql_dataset_source_type (str): sql dataset source type
        sql_linkedservice (str): sql linked service for stored procedure
        schema (str): the schema of the tables
        error_sp (str): name of error log stored procedure
        wm_sp (str): name of watermark stored procedure
        archiv_dataset (str): archiv dataset name
        temp_dataset (str): temp dataset name
        archiv_source_type (str): archiv source type
        archiv_sink_type (str): archiv sink type
        pipeline_params (list): parameter of the pipeline
        factory_name_auto (_type_): name of the data factory
        resource_name_auto (_type_): name of the resource group

    Returns:
        _type_: Pipelines for csv files according to the given template in a dictionary with according names. The Pipeline objects are needed for pulumi to wait
                for the creation of one resources before creating another one.
    """
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
    """create the execute activities, to act as parent for child pipelines
        Parameter: delta_load
        
    Args:
        pipelines (list): list of pipelines

    Returns:
        _type_: list of ExecutePipelineActivity
    """
    df_pipelines =pd.DataFrame(pipelines)
    activities = []

    for i in range(len(df_pipelines)):
                    if i == 0:
                        exe_PL = create_ExecutePipelineActivity(name=df_pipelines.loc[i,"pipeline_name"]+"_WoC",
                                                                pipeline_ref_name=df_pipelines.loc[i,"pipeline_obj"].name,
                                                                pipeline_ref_type=pipreftype,
                                                                wait_on_completion=True,
                                                                parameters=[Param("deltaload", "@pipeline().parameters.deltaload", "Expression")])
                        activities.append(exe_PL)
                    elif i >= 1:
                        exe_PL = create_ExecutePipelineActivity(name=df_pipelines.loc[i,"pipeline_name"]+"_WoC",
                                                                pipeline_ref_name=df_pipelines.loc[i,"pipeline_obj"].name,
                                                                pipeline_ref_type=pipreftype,
                                                                wait_on_completion=True,
                                                                parameters=[Param("deltaload", "@pipeline().parameters.deltaload", "Expression")],
                                                                depends_on=[depend_on(df_pipelines.loc[i-1,'pipeline_name']+"_WoC", succeeded)])
                        activities.append(exe_PL)
    return activities