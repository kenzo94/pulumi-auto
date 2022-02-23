# Introduction 
A Studentproject between the HTW - Berlin and Ceteris AG to automate the creation of Data Pipelines in Azure. 

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Dependencies
2.	Project Structure
3.	Documentation

# Dependencies

## Python
- This Code was written with Python 3.9. For pulumi you will need at least Python 3.6.

## Pulumi 
- To create azure pipelines with python code you will need to install pulumi. The following instructions are for Windows Powershell. Please see the following guide for other systems: https://www.pulumi.com/docs/get-started/azure/begin/
1. Install Pulumi over the cmd 
> choco install pulumi
2. Install the Azure CLI for authentication between azure and pulumi
> https://docs.microsoft.com/en-us/cli/azure/install-azure-cli
3. Login to your azure account
> az login
4. Create your first Pulumi programm
> pulumi new azure-python

## Pulumi Stack
- To learn different possibilities how to configure stack, please visit: https://www.pulumi.com/docs/intro/concepts/state/#logging-into-the-azure-blob-storage-backend
- In this project we Pulumi with an Azure Blob Storage backend:
1. Create and configure the Azure Blob Storage backend (can be created <a href='https://www.techwatching.dev/posts/pulumi-azure-backend'>manually</a> or with local pulumi service using this code):
1) create resource group:
    >resource_group = resources.ResourceGroup('resource_group_account_manager',
    >resource_group_name='pulumiAccountManager')

> Create an Azure resource (Storage Account):
    account_source = storage.StorageAccount('account_plmanager',   
        account_name='plmanager',
        resource_group_name=resource_group.name,
        sku=storage.SkuArgs(
            name=storage.SkuName.STANDARD_LRS,
        ),
        kind=storage.Kind.STORAGE_V2)

> BlobContainer:
    blob_container = azure_native.storage.BlobContainer("blob_container_plmanager",
        account_name=account_source.name,
        container_name="contplmanager",
        resource_group_name=resource_group.name)
2. Set local variables:
> name of the storage account, which was created recently (set via terminal):
    export AZURE_STORAGE_ACCOUNT='plmanager'
> access key of the storage account, which was created recently (set via terminal):
    export AZURE_STORAGE_KEY='9jzAQXBHun5lxXk0rj9yjy6K3vM5pMsUNO2J4r5lhT3eSZTLK0CZyQpYa8aNGexTjV1xMpz7e//87diog8fUww=='
3. Login login to created container
    pulumi login azblob://contplmanager
4. Use pulimi up command to create new stack (e.g. htw_dev) and give it PASSPHRASE (do not lose it! e.g. htw_dev). Do not pulish changes.
5. In the created yaml file (e.g. Pulumi.htw_dev.yaml) add following configurations:
  azure-native:location: WestEurope
6. Set PULUMI_CONFIG_PASSPHRASE as local variable
    export PULUMI_CONFIG_PASSPHRASE='htw_dev'
7. You can access created stack via pulumi up



## Configure ODBC
### MacOS
-  Install Microsoft ODBC Driver 17 for SQL Server:
> /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
> brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
> brew update
> HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install msodbcsql17 mssql-tools
- Start venv
> source [PFAD]/venv/bin/activate
- Install ODBC in venv
> pip install pyodbc
- Install openssl 1.1
> brew install openssl@1.1
- If errors occure please visit this page
-----https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/known-issues-in-this-version-of-the-driver?view=sql-server-ver15 
> rm -rf /usr/local/opt/openssl
> version=$(ls /usr/local/Cellar/openssl@1.1 | grep "1.1")
> ln -s /usr/local/Cellar/openssl@1.1/$version /usr/local/opt/openssl

### Windows
1. Install latest Python Version: https://www.python.org/downloads/windows/
2. Install the latest Microsoft ODBC Driver for SQL Server on Windows: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15
3. Install pyodbc
> pip install pyodbc
4. Test connection to Database
> server = 'xxx'\
> database = 'xxx' \
> username = 'xxx' \
> password = 'xxx' \
> cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password) \
> cursor = cnxn.cursor() 

> cursor.execute("select * from Your.Table") \
> row = cursor.fetchone() \
> if row: \
>    print(row) 

- For any future updates please refer to this installation guide: https://docs.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-ver15
- For further information about pyodbc please check the github: https://github.com/mkleehammer/pyodbc/wiki/Getting-started

# Project Structure
- <b>ADF Folder</b>: In this folder you will fine the manual created pipeline in azure, which works as a template for the automated pipeline creation.
- <b>pulumi_auto</b>: In this folder you will fine the framework to create pipelines with the help of the pulumi package

# Documentation
- <b>Pulumi</b>:
    - <b>Datafactory</b> (All Resources): https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/
    - <b>Pipelines</b> (Pipeline Resources): https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/pipeline/