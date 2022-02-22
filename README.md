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
> ulumi new azure-python

## Configure odbc (TO BE EDITED)
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
- <b>ADF Folder: In this folder you will fine the manual created pipeline in azure, which works as a template for the automated pipeline creation.
- <b>pulumi_auto: In this folder you will fine the framework to create pipelines with the help of the pulumi package

# Documentation
- <b>Pulumi:
    - <b>Datafactory (All Resources): https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/
    - <b>Pipelines (Pipeline Resources): https://www.pulumi.com/registry/packages/azure-native/api-docs/datafactory/pipeline/