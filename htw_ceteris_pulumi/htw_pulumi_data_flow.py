import pulumi
import pulumi_azure_native as azure_native

"""
    :param tableName string: name of table for which this dataflow will be created
    :param tableID string: key column (only one name is accepted)
    :param factory_name_auto string: name of factory, where dataflow will be created
    :param resource_group_name_auto string: name of resource group, where dataflow will be created
    :param linked_service_datalake object: datalake object
    
    :return created data flow object
"""

def createDataFlowAndReturn(tableName,tableID,factory,resource_group,linked_service_datalake,dataset_dl_temp):
    
    data_flow = azure_native.datafactory.DataFlow("dataFlow"+tableName,
        data_flow_name="DF_Import_ADLSTemp"+tableName,
        factory_name=factory.name,
        properties=azure_native.datafactory.MappingDataFlowArgs(
            folder = {
                "name": "Import"
            },
            description="",
            script = """
            parameters{
                tableID as string ('"""+tableID+"""'),
                tableName as string ('"""+tableName+"""')
            }
            source(allowSchemaDrift: true,
                validateSchema: false,
                ignoreNoFilesFound: false,
                format: 'parquet',
                wildcardPaths:[(concat($tableName,'.parquet'))]) ~> SRCADLTemp"""+tableName+"""
            source(allowSchemaDrift: true,
                validateSchema: false,
                ignoreNoFilesFound: true,
                format: 'delta',
                compressionType: 'snappy',
                compressionLevel: 'Fastest',
                fileSystem: 'import',
                folderPath: ($tableName)) ~> SRCADLImport"""+tableName+"""
            SRCADLTemp"""+tableName+""", SRCADLImport"""+tableName+""" lookup(toString(byName($tableID,"SRCADLTemp"""+tableName+"""")) == toString(byName($tableID,"SRCADLImport"""+tableName+"""")),
                multiple: false,
                pickup: 'any',
                broadcast: 'auto')~> LKP"""+tableID+"""Import"""+tableName+"""
            LKP"""+tableID+"""Import"""+tableName+""" split(isNull(toString(byName($tableID,"LKP"""+tableID+"""Import"""+tableName+""""))),
                !isNull(toString(byName($tableID,"LKP"""+tableID+"""Import"""+tableName+""""))),
                disjoint: false) ~> CSP@(CSPinsert, CSPupdate)
            CSP@CSPupdate alterRow(updateIf(true())) ~> ALTupdate
            CSP@CSPinsert alterRow(insertIf(true())) ~> ALTinsert
            ALTinsert sink(allowSchemaDrift: true,
                validateSchema: false,
                format: 'delta',
                fileSystem: 'import',
                folderPath: ($tableName),
                mergeSchema: false,
                autoCompact: false,
                optimizedWrite: false,
                vacuum: 0,
                deletable:false,
                insertable:true,
                updateable:false,
                upsertable:false,
                umask: 0022,
                preCommands: [],
                postCommands: [],
                skipDuplicateMapInputs: true,
                skipDuplicateMapOutputs: true) ~> DSTADLInsertImport"""+tableName+"""
            ALTupdate sink(allowSchemaDrift: true,
                validateSchema: false,
                format: 'delta',
                fileSystem: 'import',
                folderPath: ($tableName),
                mergeSchema: false,
                autoCompact: false,
                optimizedWrite: false,
                vacuum: 0,
                deletable:false,
                insertable:false,
                updateable:true,
                upsertable:false,
                keys:[($tableID)],
                umask: 0022,
                preCommands: [],
                postCommands: [],
                skipDuplicateMapInputs: true,
                skipDuplicateMapOutputs: true) ~> DSTADLUpdateImport"""+tableName+"""""",
            sources=[
                azure_native.datafactory.DataFlowSourceArgs(
                    dataset=azure_native.datafactory.DatasetReferenceArgs(
                        reference_name=dataset_dl_temp.name,
                        type="DatasetReference",
                    ),
                    name="SRCADLTemp"+tableName,
                ),
                azure_native.datafactory.DataFlowSourceArgs(
                    linked_service=azure_native.datafactory.LinkedServiceReferenceArgs(
                        reference_name=linked_service_datalake.name,
                        type="LinkedServiceReference",
                    ),
                    name="SRCADLImport"+tableName,
                ),
            ],
            sinks=[
                azure_native.datafactory.DataFlowSinkArgs(
                    linked_service =azure_native.datafactory.LinkedServiceReferenceArgs(
                        reference_name=linked_service_datalake.name,
                        type="LinkedServiceReference",
                    ),
                    name="DSTADLInsertImport"+tableName,
                ),
                azure_native.datafactory.DataFlowSinkArgs(
                    linked_service=azure_native.datafactory.LinkedServiceReferenceArgs(
                        reference_name=linked_service_datalake.name,
                        type="LinkedServiceReference",
                    ),
                    name="DSTADLUpdateImport"+tableName,
                ),
            ],
            transformations=[
                    azure_native.datafactory.TransformationArgs(
                        name = "LKP"+tableID+"Import"+tableName
                    ),
                    azure_native.datafactory.TransformationArgs(
                        name = "CSP"
                    ),
                    azure_native.datafactory.TransformationArgs(
                        name = "ALTupdate"
                    ),
                    azure_native.datafactory.TransformationArgs(
                        name = "ALTinsert"
                    ),                
            ],
            type="MappingDataFlow",
        ),
        resource_group_name = resource_group.name)
    return data_flow
