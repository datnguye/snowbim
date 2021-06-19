# snowbim
This is to do something awesome between snowflake database and Power BI tabular model file (.bim).

Those are:
* Refresh tables (key: name)
* Refresh table's columns (key: name)
* Refresh table's partitions (key: power-query or name)

Supported Models:
* Compatibility Level: 1550
* Default Power BI Data Source Version: powerBI_V3

We make use of [dbt (data build tool)'s profile](https://docs.getdbt.com/dbt-cli/configure-your-profile) to configure the Snowflake connection.
> Therefore, PLEASE MAKE SURE you've already managed to create 1 dbt profiles.yml file.

Installation:
```
python -m pip install snowbim --upgrade

# dependencies
python -m pip install snowflake-connector-python[pandas]

# check version
python -m snowbim --version
```


## Usage
```
python -m snowbim --help
```

If you don't use --profile-dir argument, by default, it will look for profiles.yml in the user home folder:
* Windows:  %userprofile%/.dbt/profiles.yml
* Linux:    ~/.dbt/profiles.yml

Sample commands:
* To create new model.bim file:
```
python -m snowbim --bim "/path/to/model.bim" --db "YOUR_SF_DB_NAME" --schema "YOUR_SF_SCHEMA_NAME"
# model.bim will be created after above command
```

* To upgrade existing model.bim file:
```
python -m snowbim --bim "/path/to/model.bim" --db "YOUR_SF_DB_NAME" --schema "YOUR_SF_SCHEMA_NAME"
# model.bim will be overidden after above command
```

* To upgrade existing model.bim file but output to a new model_upgrade.bim file:
```
python -m snowbim --bim "/path/to/model.bim" --out "/path/to/model_upgrade.bim" --db "YOUR_SF_DB_NAME" --schema "YOUR_SF_SCHEMA_NAME"
# model_upgrade.bim will be created after above command
```

> NOTE: If schema is up-to-date, .bim file will not be created or modified.


## Development Enviroment
Virtual enviroment:
```
python -m venv env
```

Activate virtual env:
```
Windows: 	.\env\Scripts\activate
Linux:		source env/bin/activate
```

Install dependencies:
```
pip install -r requirements.txt
```


## TODO:
* Adding schema list


