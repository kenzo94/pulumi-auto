"""An Azure RM Python Pulumi program"""

#main.py is the Pulumi program that defines our stack resources.

import pulumi
import pulumi_azure_native as azure_native
from pulumi_azure_native import storage
from pulumi_azure_native import resources
from linkedService import createLSSourceASQLandReturn
from linkedService import createLSABLBandReturn
from linkedService import createLSTargetADLSandReturn
#Gedanken über Benamung
#import pulumi_azure as Azure

# Create an Azure Resource Group This Pulumi program creates an Azure resource group and storage account and then exports the storage account’s primary key.
resource_group = resources.ResourceGroup('resource_group',
 resource_group_name='pulumiauto')

# Create an Azure resource (Storage Account)
account_source = storage.StorageAccount('cethtwstorage',   
    account_name='plsource1',
    resource_group_name=resource_group.name,
    sku=storage.SkuArgs(
        name=storage.SkuName.STANDARD_LRS,
    ),
    kind=storage.Kind.STORAGE_V2)

    #BlobContainer
blob_container = azure_native.storage.BlobContainer("blobContainer",
    account_name=account_source.name,
    container_name="contpl1",
    default_encryption_scope="encryptionscope185", #notwendig?
    deny_encryption_scope_override=True,
    resource_group_name=resource_group.name)


account_dest = storage.StorageAccount('cethtwstorage3', 
    account_name='pldestination',
    resource_group_name=resource_group.name,
    sku=storage.SkuArgs(
        name=storage.SkuName.STANDARD_LRS,
    ),
    kind=storage.Kind.STORAGE_V2)



# Export the primary key of the Storage Account - kann später dynamisch übergeben werden für Linked Services für accout 2
primary_key = pulumi.Output.all(resource_group.name, account_source.name) \
    .apply(lambda args: storage.list_storage_account_keys(
        resource_group_name=args[0],
        account_name=args[1]
    )).apply(lambda accountKeys: accountKeys.keys[0].value)

pulumi.export("primary_storage_key", primary_key)

#Azure Datafactory
factory = azure_native.datafactory.Factory("factory",
    factory_name="htwcetdatafactory",
    resource_group_name=resource_group.name)



#SqlServer

server = azure_native.sql.Server("server",
    administrator_login="Team4Admin",
    administrator_login_password="OZh2fwL3TUqSzFO0fwfc", #später lösen, sodass sie nicht mehr im sourcecode sind
    resource_group_name=resource_group.name,
    server_name="htw-cet-sqlserver",
    public_network_access = "Enabled",
    minimal_tls_version="1.2")


#Database
database = azure_native.sql.Database("database",
    database_name="DWH",
    #kind="v12.0,user,vcore,serverless",
    resource_group_name=resource_group.name,
    server_name=server.name,
    sku=azure_native.sql.SkuArgs(
        capacity=6,
        family="Gen5",
        name="GP_S_Gen5",
        tier="GeneralPurpose"
    ),
    collation="SQL_Latin1_General_CP1_CI_AS",
    catalog_collation="SQL_Latin1_General_CP1_CI_AS",
    auto_pause_delay=60,
    min_capacity=1,
    requested_backup_storage_redundancy="Local"
    
    )

database = azure_native.sql.Database("dbsource1",
    database_name="DBSource1",
    #kind="v12.0,user,vcore,serverless",
    resource_group_name=resource_group.name,
    server_name=server.name,
    sku=azure_native.sql.SkuArgs(
        capacity=6,
        family="Gen5",
        name="GP_S_Gen5",
        tier="GeneralPurpose"
    ),
    collation="SQL_Latin1_General_CP1_CI_AS",
    auto_pause_delay=60,
    min_capacity=1,
    catalog_collation="SQL_Latin1_General_CP1_CI_AS",
    requested_backup_storage_redundancy="Local",
    sample_name="AdventureWorksLT"
    )

    #ConfigDatenbank Basic Tier 2-3 tabellen aufnehmen (Linked Service)

#Hanna: Pipeline erst ohne Parameter
#general info
factory_name_auto = "htwcetdatafactory"
resource_group_name_auto="pulumiauto"
psw="OZh2fwL3TUqSzFO0fwfc"
userid="Team4Admin"

#Source auto
source_server_name_sql_auto ="htw-cet-sqlserver"
source_server_port_sql_auto = "1433"
source_server_alias_sql_auto = "DBSource1"

# Blob auto
blob_account_name_auto = "plsource1"
blob_account_key_auto = "amu31xt5j91azbjZsACM5VaDMIIEnIj8Y3aYOKmX1aCbdLRCJxI/lLNpbno/X1nKHlPxyYfi3v3aSakEKB6Gpw=="

# Datalake auto
data_lake_account_name_auto = "pldestination"
data_lake_account_key_auto = "SOKULBYhq4QRX6XeVDKZTnoYffi9iPZeEmKqLArrt+5gPl+MP+46AOp3V6r/wBxq2jEK+xiZd87v1/XJILwk1A=="


#linked service names
linked_service_name_sql="LS_ASQL_SalesLT"
linked_service_name_blob ="LS_ABLB_CSV"
linked_service_name_datalake ="LS_ADLS_Target"


# create linked services
linked_service_sql_db2 = createLSSourceASQLandReturn(factory_name_auto,linked_service_name_sql,source_server_name_sql_auto,source_server_port_sql_auto,source_server_alias_sql_auto,userid,psw,resource_group_name_auto)
linked_service_blob =createLSABLBandReturn(factory_name_auto,linked_service_name_blob,blob_account_name_auto, blob_account_key_auto,resource_group_name_auto)
linked_service_datalake = createLSTargetADLSandReturn(factory_name_auto,linked_service_name_datalake,data_lake_account_name_auto, data_lake_account_key_auto,resource_group_name_auto)

# create datasets
dataset_source_db_name = "DS_ASQL_DB"
dataset_blob_name = "DS_ABLB_Email"
dataset_data_lake_import_name  = "DS_ADLS_Import"
dataset_data_lake_archiv_name  = "DS_ADLS_Archiv"
dataset_data_lake_temp_name  = "DS_ADLS_Temp"

dataset_asql = azure_native.datafactory.Dataset("dataset",
    dataset_name= dataset_source_db_name,
    factory_name=factory_name_auto,
    properties=azure_native.datafactory.AzureSqlTableDatasetArgs(
        folder= {
            "name": "SalesLT"
        },
        linked_service_name=azure_native.datafactory.LinkedServiceReferenceArgs(
            reference_name= linked_service_sql_db2.name,
            type= "LinkedServiceReference"
        ),
        type="AzureSqlTable",
    ),
    resource_group_name=resource_group_name_auto)

dataset_blob = azure_native.datafactory.Dataset("datasetBlob",
    dataset_name=dataset_blob_name,
    factory_name=factory_name_auto,
    properties=azure_native.datafactory.DelimitedTextDatasetArgs(
        folder= {
            "name": "CSV"
        },
        linked_service_name=azure_native.datafactory.LinkedServiceReferenceArgs(
            reference_name= linked_service_blob.name,
            type= "LinkedServiceReference"
        ),
        location = azure_native.datafactory.AzureBlobStorageLocationArgs(
            type ="AzureBlobStorageLocation",
            file_name = "email.csv",
            container = "source",
        ),
        column_delimiter = ";",
        escape_char= "\\",
        first_row_as_header = True,
        quote_char= "\"",
        
        type="DelimitedText",
    ),
    resource_group_name=resource_group_name_auto)

dataset_dl_archiv = azure_native.datafactory.Dataset("datasetDLArchiv",
    dataset_name=dataset_data_lake_archiv_name,
    factory_name=factory_name_auto,
    properties=azure_native.datafactory.ParquetDatasetArgs(
        folder= {
            "name": "DataLake"
        },
        linked_service_name=azure_native.datafactory.LinkedServiceReferenceArgs(
            reference_name= linked_service_datalake.name,
            type= "LinkedServiceReference"
        ),
        parameters= {
            "filename":{
                "type": "string"
            }
        },
        location = azure_native.datafactory.AzureBlobFSLocationArgs(
            type ="AzureBlobFSLocation",
            file_name = {
                    "value": "@dataset().filename",
                    "type": "Expression"
                },
            file_system = "archiv"
        ),
        compression_codec= "snappy",
        type="Parquet",
    ),
    resource_group_name=resource_group_name_auto)

dataset_dl_import = azure_native.datafactory.Dataset("datasetDLImport",
    dataset_name=dataset_data_lake_import_name,
    factory_name=factory_name_auto,
    properties=azure_native.datafactory.ParquetDatasetArgs(
        folder= {
            "name": "DataLake"
        },
        linked_service_name=azure_native.datafactory.LinkedServiceReferenceArgs(
            reference_name= linked_service_datalake.name,
            type= "LinkedServiceReference"
        ),
        parameters= {
            "foldername": {
                "type": "string"
            }
        },
        location = azure_native.datafactory.AzureBlobFSLocationArgs(
            type ="AzureBlobFSLocation",
            folder_path = {
                    "value": "@dataset().foldername",
                    "type": "Expression"
                },
            file_system = "import"
        ),
        compression_codec= "snappy",
        type="Parquet",
    ),
    resource_group_name=resource_group_name_auto)
    
dataset_dl_temp = azure_native.datafactory.Dataset("datasetDLTemp",
    dataset_name=dataset_data_lake_temp_name,
    factory_name=factory_name_auto,
    properties=azure_native.datafactory.ParquetDatasetArgs(
        folder= {
            "name": "DataLake"
        },
        linked_service_name=azure_native.datafactory.LinkedServiceReferenceArgs(
            reference_name= linked_service_datalake.name,
            type= "LinkedServiceReference"
        ),
        parameters= {
            "filename":{
                "type": "string"
            }
        },
        location = azure_native.datafactory.AzureBlobFSLocationArgs(
            type ="AzureBlobFSLocation",
            file_name = {
                    "value": "@dataset().fileName",
                    "type": "Expression"
                },
            file_system = "temp"
        ),
        compression_codec= "snappy",
        type="Parquet",
    ),
    resource_group_name=resource_group_name_auto)

# create dataflows
data_flow_csv = azure_native.datafactory.DataFlow("dataFlowcsv",
    data_flow_name="DF_Import_ADLSTempEmail",
    factory_name=factory_name_auto,
    properties=azure_native.datafactory.MappingDataFlowArgs(
        folder = {
            "name": "Import"
        },
        description="",
        script = """source(output(
                    Loginemail as string,
                    Identifier as string,
                    Firstname as string,
                    Lastname as string
                ),
                allowSchemaDrift: true,
                validateSchema: false,
                ignoreNoFilesFound: false,
                format: 'parquet',
                wildcardPaths:['Email.parquet']) ~> SRCADLTempEmail
            source(output(
                    Loginemail as string,
                    Identifier as string,
                    Firstname as string,
                    Lastname as string
                ),
                allowSchemaDrift: true,
                validateSchema: false,
                ignoreNoFilesFound: true,
                format: 'delta',
                compressionType: 'snappy',
                compressionLevel: 'Fastest',
                fileSystem: 'import',
                folderPath: 'Email') ~> SRCADLImportEmail
            SRCADLTempEmail, SRCADLImportEmail lookup(SRCADLTempEmail@Identifier == SRCADLImportEmail@Identifier,
                multiple: false,
                pickup: 'any',
                broadcast: 'auto')~> LKPIdentifierImportEmail
            LKPIdentifierImportEmail split(isNull(SRCADLImportEmail@Identifier),
                !isNull(SRCADLImportEmail@Identifier),
                disjoint: false) ~> CSP@(CSPinsert, CSPupdate)
            CSP@CSPupdate alterRow(updateIf(true())) ~> ALTupdate
            CSP@CSPinsert alterRow(insertIf(true())) ~> ALTinsert
            ALTinsert sink(allowSchemaDrift: true,
                validateSchema: false,
                format: 'delta',
                fileSystem: 'import',
                folderPath: 'Email',
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
                skipDuplicateMapOutputs: true) ~> DSTADLInsertImportEmail
            ALTupdate sink(allowSchemaDrift: true,
                validateSchema: false,
                format: 'delta',
                fileSystem: 'import',
                folderPath: 'Email',
                mergeSchema: false,
                autoCompact: false,
                optimizedWrite: false,
                vacuum: 0,
                deletable:false,
                insertable:false,
                updateable:true,
                upsertable:false,
                keys:['Identifier'],
                umask: 0022,
                preCommands: [],
                postCommands: [],
                skipDuplicateMapInputs: true,
                skipDuplicateMapOutputs: true) ~> DSTADLUpdateImportEmail""",
        sources=[
            azure_native.datafactory.DataFlowSourceArgs(
                dataset=azure_native.datafactory.DatasetReferenceArgs(
                    reference_name="DS_ADLS_Temp",
                    type="DatasetReference",
                ),
                name="SRCADLTempEmail",
            ),
            azure_native.datafactory.DataFlowSourceArgs(
                linked_service=azure_native.datafactory.LinkedServiceReferenceArgs(
                    reference_name=linked_service_datalake.name,
                    type="LinkedServiceReference",
                ),
                name="SRCADLImportEmail",
            ),
        ],
        sinks=[
            azure_native.datafactory.DataFlowSinkArgs(
                linked_service =azure_native.datafactory.LinkedServiceReferenceArgs(
                    reference_name=linked_service_datalake.name,
                    type="LinkedServiceReference",
                ),
                name="DSTADLInsertImportEmail",
            ),
            azure_native.datafactory.DataFlowSinkArgs(
                linked_service=azure_native.datafactory.LinkedServiceReferenceArgs(
                    reference_name=linked_service_datalake.name,
                    type="LinkedServiceReference",
                ),
                name="DSTADLUpdateImportEmail",
            ),
        ],
        transformations=[
                azure_native.datafactory.TransformationArgs(
                    name = "LKPIdentifierImportEmail"
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
    resource_group_name = resource_group_name_auto)


data_flow_db_address = azure_native.datafactory.DataFlow("dataFlowDbAddress",
    data_flow_name="DF_Import_ADLSTempAddress",
    factory_name=factory_name_auto,
    properties=azure_native.datafactory.MappingDataFlowArgs(
        folder = {
            "name": "Import"
        },
        description="",
        script = """source(output(
		AddressID as integer,
		AddressLine1 as string,
		AddressLine2 as string,
		City as string,
		StateProvince as string,
		CountryRegion as string,
		PostalCode as string,
		rowguid as string,
		ModifiedDate as timestamp
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet',
	wildcardPaths:['Address.parquet']) ~> SRCADLTempAddress
source(output(
		AddressID as integer,
		AddressLine1 as string,
		AddressLine2 as string,
		City as string,
		StateProvince as string,
		CountryRegion as string,
		PostalCode as string,
		rowguid as string,
		ModifiedDate as timestamp
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: true,
	format: 'delta',
	compressionType: 'snappy',
	compressionLevel: 'Fastest',
	fileSystem: 'import',
	folderPath: 'Address') ~> SRCADLImportAddress
SRCADLTempAddress, SRCADLImportAddress lookup(SRCADLTempAddress@AddressID == SRCADLImportAddress@AddressID,
	multiple: false,
	pickup: 'any',
	broadcast: 'auto')~> LKPAddressIDImportAddress
LKPAddressIDImportAddress split(isNull(SRCADLImportAddress@AddressID),
	!isNull(SRCADLImportAddress@AddressID),
	disjoint: false) ~> CSP@(CSPinsert, CSPupdate)
CSP@CSPupdate alterRow(updateIf(true())) ~> ALTupdate
CSP@CSPinsert alterRow(insertIf(true())) ~> ALTinsert
ALTinsert sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'delta',
	fileSystem: 'import',
	folderPath: 'Address',
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
	skipDuplicateMapOutputs: true) ~> DSTADLInsertImportAddress
ALTupdate sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'delta',
	fileSystem: 'import',
	folderPath: 'Address',
	mergeSchema: false,
	autoCompact: false,
	optimizedWrite: false,
	vacuum: 0,
	deletable:false,
	insertable:false,
	updateable:true,
	upsertable:false,
	keys:['AddressID'],
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> DSTADLUpdateImportAddress""",
        sources=[
            azure_native.datafactory.DataFlowSourceArgs(
                dataset=azure_native.datafactory.DatasetReferenceArgs(
                    reference_name="DS_ADLS_Temp",
                    type="DatasetReference",
                ),
                name= "SRCADLTempAddress",
            ),
            azure_native.datafactory.DataFlowSourceArgs(
                linked_service=azure_native.datafactory.LinkedServiceReferenceArgs(
                    reference_name=linked_service_datalake.name,
                    type="LinkedServiceReference",
                ),
                name="SRCADLImportAddress",
            ),
        ],
        sinks=[
            azure_native.datafactory.DataFlowSinkArgs(
                linked_service =azure_native.datafactory.LinkedServiceReferenceArgs(
                    reference_name=linked_service_datalake.name,
                    type="LinkedServiceReference",
                ),
                name="DSTADLInsertImportAddress",
            ),
            azure_native.datafactory.DataFlowSinkArgs(
                linked_service=azure_native.datafactory.LinkedServiceReferenceArgs(
                    reference_name=linked_service_datalake.name,
                    type="LinkedServiceReference",
                ),
                name="DSTADLUpdateImportAddress",
            ),
        ],
        transformations=[

                azure_native.datafactory.TransformationArgs(
                    name = "LKPAddressIDImportAddress"
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
    resource_group_name = resource_group_name_auto)

data_flow_db_product = azure_native.datafactory.DataFlow("dataFlowDbProduct",
    data_flow_name="DF_Import_ADLSTempProduct",
    factory_name=factory_name_auto,
    properties=azure_native.datafactory.MappingDataFlowArgs(
        folder = {
            "name": "Import"
        },
        description="",
        script = """source(output(
                ProductID as integer,
                Name as string,
                ProductNumber as string,
                Color as string,
                StandardCost as decimal(19,4),
                ListPrice as decimal(19,4),
                Size as string,
                Weight as decimal(8,2),
                ProductCategoryID as integer,
                ProductModelID as integer,
                SellStartDate as timestamp,
                SellEndDate as timestamp,
                DiscontinuedDate as timestamp,
                ThumbNailPhoto as binary,
                ThumbnailPhotoFileName as string,
                rowguid as string,
                ModifiedDate as timestamp
            ),
            allowSchemaDrift: true,
            validateSchema: false,
            ignoreNoFilesFound: false,
            format: 'parquet',
            wildcardPaths:['Product.parquet']) ~> SRCADLTempProduct
        source(output(
                ProductID as integer,
                Name as string,
                ProductNumber as string,
                Color as string,
                StandardCost as decimal(19,4),
                ListPrice as decimal(19,4),
                Size as string,
                Weight as decimal(8,2),
                ProductCategoryID as integer,
                ProductModelID as integer,
                SellStartDate as timestamp,
                SellEndDate as timestamp,
                DiscontinuedDate as timestamp,
                ThumbNailPhoto as binary,
                ThumbnailPhotoFileName as string,
                rowguid as string,
                ModifiedDate as timestamp
            ),
            allowSchemaDrift: true,
            validateSchema: false,
            ignoreNoFilesFound: true,
            format: 'delta',
            compressionType: 'snappy',
            compressionLevel: 'Fastest',
            fileSystem: 'import',
            folderPath: 'Product') ~> SRCADLImportProduct
        SRCADLTempProduct, SRCADLImportProduct lookup(SRCADLTempProduct@ProductID == SRCADLImportProduct@ProductID,
            multiple: false,
            pickup: 'any',
            broadcast: 'auto')~> LKPProductIDImportProduct
        LKPProductIDImportProduct split(isNull(SRCADLImportProduct@ProductID),
            !isNull(SRCADLImportProduct@ProductID),
            disjoint: false) ~> CSP@(CSPinsert, CSPupdate)
        CSP@CSPupdate alterRow(updateIf(true())) ~> ALTupdate
        CSP@CSPinsert alterRow(insertIf(true())) ~> ALTinsert
        ALTinsert sink(allowSchemaDrift: true,
            validateSchema: false,
            format: 'delta',
            fileSystem: 'import',
            folderPath: 'Product',
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
            skipDuplicateMapOutputs: true) ~> DSTADLInsertImportProduct
        ALTupdate sink(allowSchemaDrift: true,
            validateSchema: false,
            format: 'delta',
            fileSystem: 'import',
            folderPath: 'Product',
            mergeSchema: false,
            autoCompact: false,
            optimizedWrite: false,
            vacuum: 0,
            deletable:false,
            insertable:false,
            updateable:true,
            upsertable:false,
            keys:['ProductID'],
            umask: 0022,
            preCommands: [],
            postCommands: [],
            skipDuplicateMapInputs: true,
            skipDuplicateMapOutputs: true) ~> DSTADLInsertUpdateProduct""",
        sources=[
            azure_native.datafactory.DataFlowSourceArgs(
                dataset=azure_native.datafactory.DatasetReferenceArgs(
                    reference_name="DS_ADLS_Temp",
                    type="DatasetReference",
                ),
                name= "SRCADLTempProduct",
            ),
            azure_native.datafactory.DataFlowSourceArgs(
                linked_service=azure_native.datafactory.LinkedServiceReferenceArgs(
                    reference_name=linked_service_datalake.name,
                    type="LinkedServiceReference",
                ),
                name="SRCADLImportProduct",
            ),
        ],
        sinks=[
            azure_native.datafactory.DataFlowSinkArgs(
                linked_service =azure_native.datafactory.LinkedServiceReferenceArgs(
                    reference_name=linked_service_datalake.name,
                    type="LinkedServiceReference",
                ),
                name="DSTADLInsertImportProduct",
            ),
            azure_native.datafactory.DataFlowSinkArgs(
                linked_service=azure_native.datafactory.LinkedServiceReferenceArgs(
                    reference_name=linked_service_datalake.name,
                    type="LinkedServiceReference",
                ),
                name="DSTADLInsertUpdateProduct",
                
            ),
        ],
        transformations=[

                azure_native.datafactory.TransformationArgs(
                    name = "LKPProductIDImportProduct"
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
    resource_group_name = resource_group_name_auto)




