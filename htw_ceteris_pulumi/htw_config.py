import pulumi

## SAVE CONFIGS FOR CURRENT SELECTED STACK
### Pulumi doc: https://www.pulumi.com/docs/intro/concepts/config/
### Pulumi Work with Secrets: https://www.pulumi.com/docs/intro/concepts/secrets/


cfg = pulumi.Config()
dbSourceUserName = cfg.require('dbSourceUserName')
dbSourcePSW = cfg.require('dbSourcePSW')
dbDWHName = cfg.require('dbDWHName')
dbSourceName = cfg.require('dbSourceName')
resourceGroupName = cfg.require('resourceGroupName')
serverName= cfg.require('serverName')
storageAccountDestinationName= cfg.require('storageAccountDestinationName')
storageAccountSourceName= cfg.require('storageAccountSourceName')
blobContainerName = cfg.require('blobContainerName')
factoryName = cfg.require('factoryName')
firewallName = cfg.require('firewallName')
blbContTargetImport = cfg.require('blobContainerNameTargetImport')
blbContTargetArchiv = cfg.require('blobContainerNameTargetArchiv')
dbStpError= cfg.require('dbStoredProcedurepError')
dbStpWatermark=cfg.require('dbStoredProcedurepWatermark')

print("Admin User Name: "+dbSourceUserName)
print("DB for Datawarehouse: "+dbDWHName)
print("DB Source: "+dbSourceName)
print("Resource Group Name: "+resourceGroupName)
print("Server Name: "+serverName)
print("Storage Destination Name: "+storageAccountDestinationName)
print("Storage Source Name: "+storageAccountSourceName)
print("Factory Name: "+factoryName)
print("Firewall Name: "+firewallName)
print("Blob Container Source Name: "+blobContainerName)
print("Blob Container Target Import Name: "+blbContTargetImport)
print("Blob Container Target Archiv Name: "+blbContTargetArchiv)
print("DB Source Stored Procedure Error: "+dbStpError)
print("DB Source Stored Procedure Watermark: "+dbStpWatermark)