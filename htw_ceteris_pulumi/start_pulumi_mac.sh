source venv/bin/activate
pip install --upgrade pip
pip install pyodbc
pip install pandas
deactivate
export AZURE_STORAGE_ACCOUNT='plmanager'
export AZURE_STORAGE_KEY='vT3p9RJ/B8JxEfN9oKdrGalgq1R1aIOTwiwTHquHA7DuhqFG5q0NVO+BvFMl47yXK3UjmSQx+S91EitrjLeBzQ=='
export PULUMI_CONFIG_PASSPHRASE='htw_dev'
pulumi login azblob://contplmanager
pulumi stack select
pulumi refresh --skip-preview 
pulumi up