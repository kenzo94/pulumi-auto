"""An Azure RM Python Pulumi program"""

#main.py is the Pulumi program that defines our stack resources.
import pulumi
import htw_pulumi_db as db
import htw_pulumi_infrastructure as infra
import htw_pulumi_adf as adf
import htw_config as cfg

## CREATE INFRASTRUCTURE IN AZURE PORTAL
### Create an Azure Resource Group
resource_group = infra.createResourceGroup(cfg.resourceGroupName)
### Create Storage Account
#### Storage Account for Source  
account_source =  infra.createStorageAccout(cfg.storageAccountSourceName, resource_group)
#### Storage Account for Destination
account_destination = infra.createStorageAccout(cfg.storageAccountDestinationName, resource_group)
###  Create Blob Container for Source
blob_container = infra.createBlobContainer(cfg.blobContainerName,resource_group, account_source)
###  Create Blob Container for Import
blob_container_import = infra.createBlobContainer(cfg.blbContTargetImport,resource_group, account_destination)
###  Create Blob Container for Archiv
blob_container_archiv = infra.createBlobContainer(cfg.blbContTargetArchiv,resource_group, account_destination)
### Create Data Factory
data_factory = infra.createDataFactory(cfg.factoryName, resource_group)
### Create Server
server = infra.createServer(cfg.serverName,resource_group,cfg.dbSourceUserName,cfg.dbSourcePSW)
### Create Firewall Rule
firewall_rule = infra.createFirewallRule(resource_group, server,cfg.firewallName)
### Create Database Source
db_source= infra.createDatabaseSource(resource_group,server,cfg.dbSourceName,"AdventureWorksLT")
### Create Datawarehouse Database (Target)
db_target= infra.createDatabaseTarget(resource_group, server,cfg.dbDWHName)

## UPLOAD LOCAL FILES TO SOURCE BLOB STORAGE
infra.saveLocalFilesIntoBlobStorage(account_source,resource_group, cfg.storageAccountSourceName ,cfg.blobContainerName,"./csv","*.csv")

## GET STORAGE ACCOUNTS KEYS
### Store Storage Account Key for Source
key_storage_account_source = infra.getAccountStorageKey(account_source,resource_group)
### Store Storage Account Key for Destination
key_storage_account_destination = infra.getAccountStorageKey(account_destination,resource_group)

## INITIALIZE DATABASE TABLES AND FILL THEM WITH DATA, CREATE STORED PROCEDURES, CREATE ADF ELEMENTS
pulumi.Output.all(server.name,db_source.name) \
        .apply(lambda args:{
            ### Create Sample Table and fill it with Dummy Data
            db.create_sample(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            ### Create System Tables for Watermark and Error Logs
            db.create_system_tables(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            ### Fill Watermark Table with Data
            db.fill_watermark_table(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            ### Fill Meta Table with Data (Includes Dummy Data & AdventureWorksLT)
            db.fill_meta_table(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            ### Create Stored Procedure to Update Watermark Table
            db.create_stored_procedure_watermark(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            ### Create Stored Procedure to Update Error Log Table
            db.create_stored_procedure_error_log(args[0],args[1],cfg.dbSourceUserName,cfg.dbSourcePSW),
            ### Trigger Creation of Azure Data Factory Elements (incl. Pipelines)
            adf.createADFElements(resource_group, account_source, account_destination, data_factory, server, db_source,key_storage_account_source,key_storage_account_destination)
        })