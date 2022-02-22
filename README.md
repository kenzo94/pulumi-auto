# Introduction 
TODO: Give a short introduction of your project. Let this section explain the objectives or the motivation behind this project. 

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)

# Configure odbc (TO BE EDITED)
## MacOS
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

## Windows
1. Install latest Python Version: https://www.python.org/downloads/windows/
2. Install the latest Microsoft ODBC Driver for SQL Server on Windows: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15
3. Install pyodbc
> pip install pyodbc
4. Test connection to Database
> server = 'xxx' \n
> database = 'xxx'
> username = 'xxx'
> password = 'xxx'
> cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
> cursor = cnxn.cursor()

> cursor.execute("select * from Your.Table")
> row = cursor.fetchone()
> if row:
>    print(row)

- For any future updates please refer to this guide: https://docs.microsoft.com/en-us/sql/connect/python/python-driver-for-sql-server?view=sql-server-ver15