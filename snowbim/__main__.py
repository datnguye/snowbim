import argparse, sys
from snowbim import snowbim
from snowbim import __NAME__, __VERSION__

def main():
    parser = argparse.ArgumentParser(prog=__NAME__)
    parser.add_argument('-v','--version', action='version', version='%(prog)s ' + __VERSION__)

    parser.add_argument('--bim',            help='File path to an input .bim file', type=str, default='')
    parser.add_argument('--out',            help='File path to an out .bim file. Optional', type=str, default='')
    parser.add_argument('--profile_dir',    help='File path to dbt profiles.yml. Optional, default to .dbt folder within User Home one', type=str, default=None)
    parser.add_argument('--profile',        help='dbt profile name. Optional, default: Fisrt profile in profiles.yml', type=str, default=None)
    parser.add_argument('--target',         help='dbt profile target. Optional, default: dev', type=str, default='dev')
    parser.add_argument('--db',             help='Snowflake database. Optional, default: DEMO_DB', type=str, default='DEMO_DB')
    parser.add_argument('--schema',         help='Snowflake database\'s schema. Optional, default: PUBLIC', type=str, default='PUBLIC')
    parser.add_argument('--tables_inc',     help='Snowflake tables inclusive. Items are in string splitted by comma. Optional, default: (all)', type=str, default='')
    parser.add_argument('--tables_exc',     help='Snowflake tables exclusive. Items are in string splitted by comma. Optional, default: (none)', type=str, default='')
    
    parser.add_argument("--replace-partition", help="Enable to replace partition(s) in tables. Value will be overwritten.", action="store_true")

    args = parser.parse_args()
    snowbim.upgrade_schema( bim_path            = args.bim, 
                            out_bim_path        = args.out,
                            profile_dir         = args.profile_dir,
                            profile             = args.profile,
                            target              = args.target,
                            db                  = args.db,
                            schema              = args.schema,
                            tables              = [x.strip() for x in args.tables_inc.split(',')] if args.tables_inc else [],
                            exclude_tables      = [x.strip() for x in args.tables_exc.split(',')] if args.tables_exc else [],
                            replace_partition   = args.replace_partition)

if __name__ == '__main__':
    main()