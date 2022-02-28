"""An Azure RM Python Pulumi program"""

import pulumi
from pulumi_azure_native import storage
from pulumi_azure_native import resources

# Create an Azure Resource Group
resource_group = resources.ResourceGroup('resource_group',
        resource_group_name='resourceGroupPulumi')

# Create an Azure resource (Storage Account)
account = storage.StorageAccount('account_source',
    account_name='accountnamepulumi',
    resource_group_name=resource_group.name,
    sku=storage.SkuArgs(
        name=storage.SkuName.STANDARD_LRS,
    ),
    kind=storage.Kind.STORAGE_V2)

# Create an Azure resource (Storage Account)
account2 = storage.StorageAccount('account_source_1',
    account_name='accountnamepulumi1',
    resource_group_name=resource_group.name,
    sku=storage.SkuArgs(
        name=storage.SkuName.STANDARD_LRS,
    ),
    kind=storage.Kind.STORAGE_V2)

# Export the primary key of the Storage Account
resource_group_name =""
account_name =""
primary_key = pulumi.Output.all(resource_group.name, account.name) \
    .apply(lambda args: storage.list_storage_account_keys(
        resource_group_name=args[0],
        account_name=args[1]
    )).apply(lambda accountKeys: accountKeys.keys[0].value)

pulumi.export("primary_storage_key", primary_key)

test = primary_key
#print(pulumi.Output.primary_storage_key)
print(resource_group_name)
print(account_name)