from snowbim.engines import snowengine, bimengine

def get_schema_changes(bim_path=None):
    print('Connecting to Snowflake...')
    conn = snowengine.connect(profile=None, target='cidevelop', db='DEV_DAT_CUSTOMER', schema='SUPPLIER')
    print('     Connected')

    print('Schema comparing...')
    changes = snowengine.compare_schema(snowflake_conn=conn[1], bim_path=bim_path)
    print('     Done')

    return changes

def upgrade_schema(bim_path=None, out_bim_path=None):
    changes = get_schema_changes(bim_path=bim_path)
    if changes[0] == 0:
        changes = changes[1]
    else:
        return (changes[0], {}, changes[2])

    print(f"Changes detected as:")
    for table in changes['model']['tables']:
        print(f"    Table: {table['name']} {'(New)' if table['is_new'] else '(Changed)'}")
        del table['is_new']
        for column in table['columns']:
            print(f"        Column: {column['name']} {'(New)' if column['is_new'] else '(Changed)'}")
            del column['is_new']
        for partition in table['partitions']:
            print(f"        Partition: {partition['name']} {'(New)' if partition['is_new'] else '(Changed)'}")
            del partition['is_new']
            
    print('Upgrading...')
    r = bimengine.upgrade_bim(file_path=bim_path, out_path=out_bim_path, changes=changes)
    print('     Done')

    return r