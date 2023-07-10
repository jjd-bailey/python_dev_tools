'''
    pip install pyyaml
    pip install polars
    pip install connectorx
    pip install pyarrow
'''
import polars
from os import (
    path,
    makedirs,
)
from yaml import (
    dump,
)
from text_formatting.text_formatting import (
    camelcase_to_snakecase,
)
from mssql.list_tables import (
    create_mssql_engine,
    get_mssql_tables,
)
from cloudformation.definitions.table_definitions import(
    create_table_definitions_dicts,
)
from cloudformation.templates.glue_jobs import (
    group_tables,
    create_glue_pyshell_jobs_dicts,
)


server_name= 'AWDEV1DA2ASQL02'
port_number= 1433
target_database= 'SDHOperational_PROD'
glue_connection= 'AdnetConnection'
sql_file_name= 'mssql_full_extract.sql'
job_type= 'full_extract'

camel_case = True

target_schema = 'Master'
#region
conn = f'mssql://GVW_AnalyticsReader:Hm555yIs1aWcLn2LtHdD@{server_name}/{target_schema}?driver=SQL+Server&trusted_connection={True}'
#endregion

#  'sdh-operational-full-grain-extract'
glue_job_name = f"{target_database.replace('_','-').lower()}-{job_type.replace('_','-').lower()}"

mssql_engine = create_mssql_engine(
    host = server_name,
    port = port_number,
    database = target_database
)

database_tables = get_mssql_tables(engine = mssql_engine)

table_sorted = sorted(
    database_tables,
    key = lambda x: (list(x.values())[0]['schema'],list(x.values())[0]['record_count']),
    reverse = True
)

"""
    Focus on desired schema
"""

target_tables = []

for q in table_sorted:
    for w in q:
        if q[w]['schema'] == target_schema:
            target_tables.append(q)

len(target_tables)


'''
    Create definitions dict
'''
resource = {}
schema_list = {}


sdho_inputs = {
    'TruckSerials': {
        'Grain': 'TruckId',
        'predicate_column1': 'DateCreated',
        'predicate_column2': 'DateUpdated',
        'ExtractType': 'delta_extract',
    },
    'PartSourceSystem': {
        'ExtractType': 'full_extract',
    },
    'IllustrationCalloutCoordinateTie': {
        'ExtractType': 'full_extract',
    },
    'IllustrationExceptionMediaTie': {
        'ExtractType': 'full_extract',
    },
    'PartIllustrationVectorMediaTie': {
        'ExtractType': 'full_extract',
    },
    'UserSetting': {
        'Grain': 'UserId',
        'predicate_column1': 'DateCreated',
        'predicate_column2': 'DateUpdated',
        'ExtractType': 'delta_extract',
    },
    'IllustrationSharedConfigurationMediaTie': {
        'ExtractType': 'full_extract',
    },
    'PartIllustrationImageMediaTie': {
        'ExtractType': 'full_extract',
    },
    'ToolUsePointLocalTie': {
        'ExtractType': 'full_extract',
    },
    'IllustrationCalloutExceptionCoordinateTie': {
        'ExtractType': 'full_extract',
    },
    'TruckProductionInfo': {
        'Grain': 'TruckId',
        'predicate_column1': 'DateCreated',
        'predicate_column2': 'DateUpdated',
        'ExtractType': 'delta_extract',
    },
}

'''
    Create iter item to check table definition
'''
table_meta = iter(target_tables)
table_data = next(table_meta)


"""
    Primary column check
"""
loop = True

while loop:
    loop = False

    for q in table_data:
        table_name = q
        grain_check = table_data[table_name]['primary_column']

    if table_name.split('.')[2] in sdho_inputs:
        """
            Manually Configured
        """
        if sdho_inputs[table_name.split('.')[2]]['ExtractType'] == 'full_extract':
            if camel_case:
                new_name = camelcase_to_snakecase(table_data[table_name]['table'])
            else:
                new_name = table_data[table_name]['table'].lower()

            if table_data[table_name]['schema'] not in schema_list:
                schema_list[table_data[table_name]['schema']] = [new_name]
            else:
                schema_list[table_data[table_name]['schema']].append(new_name)

            resource[new_name] = {
                'sql': 'mssql_full_extract.sql',
                'parameters': {
                    'database': table_data[table_name]['database'],
                    'schema': table_data[table_name]['schema'],
                    'table': table_data[table_name]['table'],
                }
            }

            next_table = True
        else:
            if camel_case:
                new_name = camelcase_to_snakecase(table_data[table_name]['table'])
            else:
                new_name = table_data[table_name]['table'].lower()

            if table_data[table_name]['schema'] not in schema_list:
                schema_list[table_data[table_name]['schema']] = [new_name]
            else:
                schema_list[table_data[table_name]['schema']].append(new_name)


            resource[new_name] = {
                'sql': sql_file_name,
                'parameters': {
                    'database': table_data[table_name]['database'],
                    'schema': table_data[table_name]['schema'],
                    'table': table_data[table_name]['table'],
                    'grain': sdho_inputs[table_name.split('.')[2]]['Grain']
                }
            }

            next_table = True
    else:
        """
            Check if programatic values can be used
        """
        if grain_check != '__NoColumnNameDefined__':
            ####
            # Get table name in snake_case
            ####
            if camel_case:
                new_name = camelcase_to_snakecase(table_data[table_name]['table'])
            else:
                new_name = table_data[table_name]['table'].lower()

            if table_data[table_name]['schema'] not in schema_list:
                schema_list[table_data[table_name]['schema']] = [new_name]
            else:
                schema_list[table_data[table_name]['schema']].append(new_name)

            resource[new_name] = {
                'sql': sql_file_name,
                'parameters': {
                    'database': table_data[table_name]['database'],
                    'schema': table_data[table_name]['schema'],
                    'table': table_data[table_name]['table'],
                    'grain': table_data[table_name]['primary_column']
                }
            }

            next_table = True
        else:
            next_table = False

    if next_table:
        f'{table_data} configured'
        grain_check = None
        table_data = next(table_meta)
        loop = True


table_data[table_name]['table']


len(target_tables)
len(resource)

"""
    Table does not have PKey defined
        pull table into python to investigate table further
"""



query = f'SELECT * FROM {table_name}'

df = polars.read_database(query, conn)

"""
    Table Count
"""
f'{int(df.select(polars.count()).item()):,}'
"""
    Headers
"""
df.head()


"""
    Identify grain
"""
int(df.select(polars.count()).item()) - int(df.select(polars.col('TruckId').n_unique()).item())

df = None
table_data = next(table_meta)

"""
    Identify tables not configured
"""
for x in target_tables:
    for y in x:
        if camelcase_to_snakecase(x[y]['table']) not in resource:
            y