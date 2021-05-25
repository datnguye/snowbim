import json
import os

def load_bim(file_path:str=None):
    '''
    Read .bim file as json object. Then, convert it to dict object
    Return a tuple (code, dict, messasge)
    '''
    bim = None
    if os.path.exists(file_path):
        return (-1, {}, 'File not found')

    with open(file_path, 'r') as f:
        bim = json.load(f)
    return (0, bim, None)
    

def upgrade_bim(file_path:str=None, out_path:str=None, changes:list=[]):
    '''
    With the changes object, make changes for dict's attributes related
    Return a tuple (code, dict, messasge)
    '''
    input_bim = load_bim(file_path=file_path)
    output_bim = {}
    if input[0] != 0:
        return (input[0], {}, input[2])
    
    # TODO

    return (0, output_bim, None)