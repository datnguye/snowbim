import json
import datetime

def json_default(o):
    '''
    Json default method
    '''
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    return o


def save_to_json_file(data:dict,file):
    '''
    Save json object to file
    '''
    with open(file, 'w', encoding='utf8') as outfile: 
        outfile.write(json.dumps(data, indent = 2, default=json_default, ensure_ascii=False))

def get_model_datatype(in_type:str=None):
    if in_type is None:
        return 'string'

    mapping = [
        ('text',            'string'),
        ('boolean',         'boolean'),
        ('timestamp_ntz',   'dateTime'),
        ('timestamp_ltz',   'dateTime'),
        ('date',            'dateTime'),
        ('number',          'double')
    ]

    datatype = [x[1] for x in mapping if x[0] == in_type.lower()]
    if datatype is None or len(datatype) == 0:
        return in_type.lower()
    else:
        datatype = datatype[0]

    return datatype
    