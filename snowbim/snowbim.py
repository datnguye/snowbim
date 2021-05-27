from snowbim.engines import snowengine, bimengine

def get_schema_changes(bim_path=None, profile:str=None, target:str=None, db:str=None, schema:str=None):
    print('Connecting to Snowflake...')
    conn = snowengine.connect(profile=profile, target=target, db=db, schema=schema)
    print('     Connected')

    print('Schema comparing...')
    changes = snowengine.compare_schema(snowflake_conn=conn[1], bim_path=bim_path)
    print('     Done')

    return changes

def upgrade_schema(bim_path=None, out_bim_path=None, profile:str=None, target:str=None, db:str=None, schema:str=None):
    changes = get_schema_changes(bim_path=bim_path, profile=profile, target=target, db=db, schema=schema)
    if changes[0] == 0:
        changes = changes[1]
    else:
        return (changes[0], {}, changes[2])

    print(f"Changes detected as: { 'Up-to-date' if len(changes['model']['tables']) == 0 else ''}")
    for table in changes['model']['tables']:
        print(f"    Table: {table['name']} {'(New)' if table['is_new'] else '(Changed)'}")
        del table['is_new']
        for column in table['columns']:
            print(f"        Column: {column['name']} {'(New)' if column['is_new'] else '(Changed)'}")
            del column['is_new']
        for partition in table['partitions']:
            print(f"        Partition: {partition['name']} {'(New)' if partition['is_new'] else '(Changed)'}")
            del partition['is_new']

    if len(changes['model']['tables']):
        print('Upgrading...')
        r = bimengine.upgrade_bim(file_path=bim_path, out_path=out_bim_path, changes=changes)
        print('     Done')
        return r

    return (0,{},None)