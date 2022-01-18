import pulumi
import pulumi_azure_native as azure_native


factory_name_auto = "htwcetdatafactory"
resource_name_auto = "pulumiauto"

pipeline_email = azure_native.datafactory.Pipeline(resource_name="PL_Import_DimABLBEmail",
                                                   pipeline_name="PL_Import_DimABLBEmail",
                                                   factory_name=factory_name_auto,
                                                   resource_group_name=resource_name_auto,
                                                   folder=azure_native.datafactory.PipelineFolderArgs(
                                                       name="Import"
                                                   ),
                                                   activities=[
                                                       azure_native.datafactory.CopyActivityArgs( #copy activity
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
                                                       azure_native.datafactory.ExecuteDataFlowActivityArgs( # dataflow activity
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
                                                       azure_native.datafactory.SqlServerStoredProcedureActivityArgs( # sp activity
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
                                                       azure_native.datafactory.SqlServerStoredProcedureActivityArgs( # sp activity
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
                                                       azure_native.datafactory.CopyActivityArgs( #copy activity
                                                           name="ABLBTempEmailToADLArchivEmail",
                                                           type="Copy",
                                                           depends_on=[
                                                               {
                                                                   "activity":"DF_Import_ADLTempEmail",
                                                                   "dependency_conditions":[
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
                                                                   "parameters":{
                                                                       "filename":{
                                                                           "value":"Email.parquet",
                                                                           "type":"Expression"
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


pipeline_master = azure_native.datafactory.Pipeline(resurce_name="PL_Import_Master",
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