import os
import json
from os.path import expanduser
import yaml
import snowflake.connector as sfconn
from yaml.loader import FullLoader

from snowbim.utilities import common


def connect(profile:str=None, project_name:str=None, target:str=None, db:str=None, schema:str=None):
    '''
    Use dbt profiles.yml to connect snowflake database
    Returns a tuple (code, snowflake connection, message)
    '''
    conn = {}

    if profile is None:
        profile = f'{expanduser("~")}/.dbt/profiles.yml'

    with open(profile, 'r') as stream:
        try:
            cred = yaml.load(stream, Loader=FullLoader)
        except yaml.YAMLError as e:
            return (-1, {}, str(e))

    if project_name is None:
        cred = cred[next(iter(cred))]
    else:
        cred = cred[project_name]

    cred = cred['outputs']
    if target is None:
        cred = cred[next(iter(cred))]
    else:
        cred = cred[target]

    if cred is None or cred['type'] != 'snowflake':
        return (-1, {}, f'Snowflake config not found: project={str(project_name)}, target={str(target)} within profile: {profile}')

    # cred = {'type': 'snowflake', 'account': 'xxx', 'user': 'xxx', 'password': 'xxx', 'role': 'xxx', 'database': 'xxx', 'warehouse': 'xxx', 'schema': 'xxx'}
    conn = sfconn.connect(
        user = cred['user'],
        password = cred['password'],
        account = cred['account'],
        warehouse = cred['warehouse'] or 'COMPUTE_WH',
        database = db or cred['database'] or 'DEMO_DB',
        schema = schema or cred['schema'] or 'PUBLIC',
        role = cred['role'] or 'PUBLIC'
    )
    
    return (0, conn, None)


def compare_schema(snowflake_conn, bim_path:str=None, mode:str='directQuery', table_excludes:list=[]):
    '''
    Get the changes of snowflake database schema
    Currently support only for
        changes = { "model": { "tables": [ { "columns": [...] }, {...} ] } }
    Returns a tuple (code, changes, message)
    '''
    # Input bim data
    in_schema = {}
    if bim_path and os.path.exists(bim_path):
        with open(bim_path, 'r') as f:
            in_schema = json.load(f)
            in_schema = in_schema['model']['tables']

    # SF bim data
    snowflake_schema = []
    cur = snowflake_conn.cursor()

    cur.execute(f'SELECT * FROM "INFORMATION_SCHEMA"."TABLES" WHERE "TABLE_SCHEMA" = \'{snowflake_conn.schema}\' ORDER BY "TABLE_SCHEMA", "TABLE_NAME"')
    df_tables = cur.fetch_pandas_all()

    cur.execute(f'SELECT * FROM "INFORMATION_SCHEMA"."COLUMNS" WHERE "TABLE_SCHEMA" = \'{snowflake_conn.schema}\' ORDER BY "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME"')
    df_columns = cur.fetch_pandas_all()

    for index, item in df_tables.iterrows():
        table_item = {
            "name": item['TABLE_NAME'],
            "is_new": 1,
            "columns": [],
            "partitions": [
                {
                    "name": f"{item['TABLE_NAME']} Partition",
                    "is_new": 1,
                    "mode": mode,
                    "source": {
                        "type": "m",
                        "expression": [
                            f"let",
                            f"    Source = Snowflake.Databases(\"{snowflake_conn.account}.snowflakecomputing.com\", \"{snowflake_conn.warehouse}\", [Role=\"{snowflake_conn.role}\", CreateNavigationProperties=null, ConnectionTimeout=null, CommandTimeout=null]),",
                            f"    {snowflake_conn.database}_Database = Source{{[Name=\"{snowflake_conn.database}\",Kind=\"Database\"]}}[Data],",
                            f"    {snowflake_conn.schema}_Schema = {snowflake_conn.database}_Database{{[Name=\"{snowflake_conn.schema}\",Kind=\"Schema\"]}}[Data],",
                            f"    #\"{item['TABLE_NAME']}_{item['TABLE_TYPE'].title()}\" = {snowflake_conn.schema}_Schema{{[Name=\"{item['TABLE_NAME']}\",Kind=\"{item['TABLE_TYPE'].title()}\"]}}[Data]",
                            f"in",
                            f"    #\"{item['TABLE_NAME']}_{item['TABLE_TYPE'].title()}\""
                        ]
                    }
                }
            ]
        }

        df_filterred_columns = df_columns.loc[df_columns['TABLE_NAME'] == item['TABLE_NAME']]
        for index, citem in df_filterred_columns.iterrows():
            table_item['columns'].append({
                "name": citem['COLUMN_NAME'],
                "is_new": 1,
                "dataType": citem['DATA_TYPE'],
                "sourceColumn": citem['COLUMN_NAME']
            })
        snowflake_schema.append(table_item)
    
    # Detect changes
    changes = { "model": { "tables": [ ] } }
    for sftable in snowflake_schema:
        in_table = [x for x in in_schema if x['name'] == sftable['name']]
        if in_table:
            # existing table
            in_table = in_table[0]

            table = {}
            table['name'] = in_table['name']
            table['is_new'] = 0

            # columns
            table['columns'] = []
            for sfcolumn in sftable['columns']:
                in_column = [x for x in in_table['columns'] if x['name'] == sfcolumn['name']]
                if in_column:
                    # existing column
                    in_column = in_column[0]
                    sf_model_type = common.get_model_datatype(sfcolumn['dataType'])
                    if (in_column['dataType'] != sf_model_type or in_column['sourceColumn'] != sfcolumn['sourceColumn']):
                        in_column['dataType'] = sf_model_type
                        in_column['sourceColumn'] = sfcolumn['sourceColumn']
                        in_column['is_new'] = 0

                        table['columns'].append(in_column)
                else:
                    # new column
                    sfcolumn['dataType'] = common.get_model_datatype(sfcolumn['dataType'])
                    table['columns'].append(sfcolumn)

            # partitions
            table['partitions'] = []
            for sfpartition in sftable['partitions']:
                in_partition = [
                    x for x in in_table['partitions']
                        if len(set(x['source']['expression']).intersection(sfpartition['source']['expression'])) == len(x['source']['expression'])
                            and x['source']['type'] == sfpartition['source']['type']
                            and x['mode'] == sfpartition['mode']
                ]
                if in_partition:
                    # existing partition
                    pass
                else:
                    # new partition
                    table['partitions'].append(sfpartition)

            if len(table['columns']) > 0 or len(table['partitions']) > 0:
                changes['model']['tables'].append(table)
        else:
            # new table
            for sfcolumn in sftable['columns']:
                sfcolumn['dataType'] = common.get_model_datatype(sfcolumn['dataType'])
            changes['model']['tables'].append(sftable)
            
    return (0, changes, None)