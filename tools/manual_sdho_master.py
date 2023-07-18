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
sdho_inputs = {
    'TruckSerials': {
        'Grain': 'TruckId',
        'create_delta': 'DateCreated',
        'update_delta': 'DateUpdated',
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
        'create_delta': 'DateCreated',
        'update_delta': 'DateUpdated',
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
        'create_delta': 'DateCreated',
        'update_delta': 'DateUpdated',
        'ExtractType': 'delta_extract',
    },
    'ExceptionLog': {
        'Grain': 'ExceptionLogId',
        'create_delta': 'Logged',
        'update_delta': 'Logged',
        'ExtractType': 'delta_extract',
    },
    'IllustrationCallout': {
        'ExtractType': 'full_extract',
    },
    'IllustrationCalloutCoordinate': {
        'ExtractType': 'full_extract',
    },
    'PartIllustration': {
        'ExtractType': 'full_extract',
    },
    'CustomerTypeTie': {
        'ExtractType': 'full_extract',
    },
    'MediaMetadata': {
        'ExtractType': 'full_extract',
    },
    'UserSiteTie': {
        'ExtractType': 'full_extract',
    },
    'SiteLayoutFlow': {
        'ExtractType': 'full_extract',
    },
    'Abbreviation': {
        'ExtractType': 'full_extract',
    },
    'UsePointGlobal': {
        'ExtractType': 'full_extract',
    },
    'SubassemblyBuildSiteRule': {
        'Grain': 'SubassemblyBuildSiteRuleId',
        'create_delta': 'StartDate',
        'update_delta': 'EndDate',
        'ExtractType': 'delta_extract',
    },
    'JobTitle': {
        'ExtractType': 'full_extract',
    },
    'Connection': {
        'ExtractType': 'full_extract',
    },
    'DepartmentRole': {
        'ExtractType': 'full_extract',
    },
    'InterSiteDeliveryLocation': {
        'ExtractType': 'full_extract',
    },
    'Division': {
        'ExtractType': 'full_extract',
    },
    'UsePointType': {
        'ExtractType': 'full_extract',
    },
    'Company': {
        'ExtractType': 'full_extract',
    },
    'PartClassification': {
        'ExtractType': 'full_extract',
    },
    'QADPartStatus': {
        'ExtractType': 'full_extract',
    },
    'SupplierRelationshipType': {
        'ExtractType': 'full_extract',
    },
    'CorporateHierarchy': {
        'ExtractType': 'full_extract',
    },
    'SmartTruckSalesCode': {
        'ExtractType': 'full_extract',
    },
    'SiteType': {
        'ExtractType': 'full_extract',
    },
    'MediaMetadataType': {
        'ExtractType': 'full_extract',
    },
    'IllustrationCalloutExceptionCoordinate': {
        'ExtractType': 'full_extract',
    },
    'PartRevisionMedia': {
        'Grain': 'PartRevisionMediaId',
        'create_delta': 'DateCreated',
        'update_delta': 'DateUpdated',
        'ExtractType': 'delta_extract',
    },
}

'''
    Create iter item to check table definition
'''
resource = {}
schema_list = {}

table_meta = iter(target_tables)
table_data = next(table_meta)


"""
    Primary column check
"""
loop = True

while loop:
    loop = False
    next_table = None

    for q in table_data:
        table_name = q
        grain_check = table_data[table_name]['primary_column']
        delta_check = table_data[table_name]['create_delta']

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
                'sql': 'mssql_delta_extract_2date_delta.sql',
                'parameters': {
                    'database': table_data[table_name]['database'],
                    'schema': table_data[table_name]['schema'],
                    'table': table_data[table_name]['table'],
                    'grain': sdho_inputs[table_name.split('.')[2]]['Grain'],
                    'create_delta': sdho_inputs[table_name.split('.')[2]]['create_delta'],
                    'update_delta': sdho_inputs[table_name.split('.')[2]]['update_delta']
                }
            }

            next_table = True
    else:
        """
            Check if programatic values can be used
        """
        if (grain_check != '__NoColumnNameDefined__' \
        and delta_check != '__NoColumnNameDefined__'):
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
                'sql': 'mssql_delta_extract_2date_delta.sql',
                'parameters': {
                    'database': table_data[table_name]['database'],
                    'schema': table_data[table_name]['schema'],
                    'table': table_data[table_name]['table'],
                    'grain': table_data[table_name]['primary_column'],
                    'create_delta': table_data[table_name]['create_delta'],
                    'update_delta': table_data[table_name]['update_delta']
                }
            }

            next_table = True
        else:
            next_table = None

    if next_table:
        f'{table_data} configured'
        grain_check = None
        table_data = next(table_meta)
        loop = True


table_data[table_name]['table']


len(target_tables)
len(resource)




body = {}
for table in resource:
    body[table] = resource[table]

###
# Check if directory exists
###
if not path.exists(f'zz_output/cloudformation/definitions/{target_database.lower()}'):
    makedirs(f'zz_output/cloudformation/definitions/{target_database.lower()}')

###
# Write config file
###
with open(f'zz_output/cloudformation/definitions/{target_database.lower()}/{target_schema.lower()}.yaml', mode = 'w') as f:
    f.write(dump(body, sort_keys = False))







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