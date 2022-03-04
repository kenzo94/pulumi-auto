import pulumi

cfg = pulumi.Config()
dbSourceUserName = cfg.require('dbSourceUserName')
dbSourcePSW = pulumi.Output.all(cfg.get_secret('dbSourcePSW'))\
    .apply(lambda args: args[0]) 
dbDWHName = cfg.require('dbDWHName')
dbSourceName = cfg.require('dbSourceName')
resourceGroupName = cfg.require('resourceGroupName')
serverName= cfg.require('serverName')
storageAccountDestinationName= cfg.require('storageAccountDestinationName')
storageAccountSourceName= cfg.require('storageAccountSourceName')


print(cfg.require('dbSourceUserName'))
print(dbSourcePSW)
print(dbDWHName)
print(dbSourceName)
print(resourceGroupName)
print(serverName)
print(storageAccountDestinationName)
print(storageAccountSourceName)