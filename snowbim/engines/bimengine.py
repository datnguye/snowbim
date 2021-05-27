import json
import os
from snowbim.utilities import common
from os.path import expanduser

def load_bim(file_path:str=None):
    '''
    Read .bim file as json object. Then, convert it to dict object
    Return a tuple (code, dict, messasge)
    '''
    bim = None
    if file_path is None or not os.path.exists(file_path):
        return (0, { "model": {} }, None)

    with open(file_path, 'r') as f:
        bim = json.load(f)
    return (0, bim, None)
    

def upgrade_bim(file_path:str=None, out_path:str=None, changes:dict={}):
    '''
    With the changes object, make changes for dict's attributes related.
    Currently support only for
        changes = { "model": { "tables": [ { "columns": [...] }, {...} ] } }
    Return a tuple (code, dict, messasge)
    '''
    input_bim = load_bim(file_path=file_path)
    if input_bim[0] != 0:
        return (input_bim[0], {}, input_bim[2])
    else: 
        input_bim = input_bim[1]

    output_bim = {
        "name": "PowerBITabularModel",
        "compatibilityLevel": 1550,
        "model": {
            "culture": "en-US",
            "dataAccessOptions": {
                "legacyRedirects": "true",
                "returnErrorValuesAsNull": "true"
            },
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "sourceQueryCulture": "en-US",
            "tables": [],
            "relationships": [],
            "cultures": [
                {
                    "name": "en-US",
                    "linguisticMetadata": {
                    "content": {
                        "Version": "1.0.0",
                        "Language": "en-US",
                        "DynamicImprovement": "HighConfidence"
                    },
                    "contentType": "json"
                    }
                }
            ],
            "annotations": []
        }
    }

    if not file_path:
        file_path = f'{expanduser("~")}/model.bim'
    if not out_path:
        out_path = file_path
    
    # completely new model
    if not input_bim['model']:
        output_bim['model']['tables'] = changes['model']['tables']
        
    # upgrade model
    else:
        output_bim = input_bim

        in_tables = input_bim['model']['tables']
        for table in changes['model']['tables']:
            change_table_name = table['name']
            in_table = [x for x in in_tables if x['name'] == change_table_name]
            if len(in_table) > 1:
                return (-1, {}, f'Invalid model as input duplicates table: {change_table_name}')

            if len(in_table) == 0:
                # new table
                in_tables.append(table)
            else:
                in_table = in_table[0]

                # existing table
                # existing table \ columns
                in_table_columns = in_tables['columns']
                for column in table['columns']:
                    change_colum_name = column['name']
                    in_colum = [x for x in in_table_columns if x['name'] == change_colum_name]
                    if len(in_colum) > 1:
                        return (-1, {}, f'Invalid model as input duplicates column: {change_colum_name}')
                        
                    if len(in_colum) == 0:
                        # existing table \ columns \ new column
                        in_table_columns.append(column)
                    else:
                        in_colum = in_colum[0]
                        # existing table \ columns \ existing column
                        in_colum['dataType'] = column['dataType'] or in_colum['dataType']
                        in_colum['isHidden'] = column['isHidden'] or in_colum['isHidden']
                        in_colum['sourceColumn'] = column['sourceColumn'] or in_colum['sourceColumn']
                
                # existing table \ partitions
                in_table_partitions = in_tables['partitions']
                for partition in table['partitions']:
                    in_partition = [
                        x for x in in_table_partitions 
                            if len(set(x['source']['expression']).intersection(partition['source']['expression'])) == len(x['source']['expression'])
                                and x['source']['type'] == partition['source']['type']
                                and x['mode'] == partition['mode']
                    ]
                    
                    if len(in_partition) == 0:
                        # existing table \ partitions \ new partition
                        in_table_partitions.append(partition)
                    else:
                        # existing table \ partitions \ existing partition
                        pass
    print(out_path)
    common.save_to_json_file(output_bim, out_path)
    return (0, output_bim, None)