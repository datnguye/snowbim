import argparse, sys
from snowbim import snowbim

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bim',        help='File path to an input .bim file', type=str, default='')
    parser.add_argument('--out',        help='File path to an out .bim file. Optional', type=str, default='')
    parser.add_argument('--profile',    help='dbt profile name. Optional', type=str, default=None)
    parser.add_argument('--target',     help='dbt profile target. Optional, default: dev', type=str, default='dev')
    parser.add_argument('--db',         help='Snowflake database. Optional, default: DEMO_DB', type=str, default='DEMO_DB')
    parser.add_argument('--schema',     help='Snowflake database\'s schema. Optional, default: PUBLIC', type=str, default='PUBLIC')

    args = parser.parse_args()
    snowbim.upgrade_schema( bim_path        = args.bim, 
                            out_bim_path    = args.out,
                            profile         = args.profile,
                            target          = args.target,
                            db              = args.db,
                            schema          = args.schema)

if __name__ == '__main__':
    main()

# Samples:
# snowbim --bim "C:\Users\PowerBITabularModel_upgrade.bim"