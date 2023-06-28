'''
    pip install pyyaml
'''

from os import (
    path,
    makedirs,
)
from yaml import (
    dump,
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

######
#  Check if directories exist and if needed, create directories
######
def check_directories(name: str):
    if not path.exists(f'output/cloudformation/definitions/{name}'):
        makedirs(f'output/cloudformation/definitions/{name}')


    if not path.exists(f'output/cloudformation/jobs/{name}'):
        makedirs(f'output/cloudformation/jobs/{name}')


def create_glue_job_artifacts(
        server_name: str,
        port_number: int,
        target_database: str,
        target_schema: str,
        glue_connection: str,
        datalake_database_name: str,
        glue_job_name: str,
        sql_file_name: str,
):


    check_directories(datalake_database_name)



    mssql_engine = create_mssql_engine(
        host = server_name,
        port = port_number,
        database = target_database
    )

    database_tables = get_mssql_tables(engine = mssql_engine, schema_list = target_schema, remove_zero_rows = True)


    table_defs = create_table_definitions_dicts(
        tables = database_tables,
        sql_file = sql_file_name,
        camel_case = True
    )

    with open(f'output/cloudformation/definitions/{datalake_database_name}/{target_schema}.yaml', mode = 'w') as f:
        f.write(dump(table_defs, sort_keys = False))





    table_groups = group_tables(
        tables = database_tables,
        chunk_size = 1e5,
        chunk_time_s = 5,
        group_time_m = 20,
        camel_case = True
    )

    glue_jobs = create_glue_pyshell_jobs_dicts(
        groups = table_groups,
        job_name = f'{glue_job_name}-{target_schema}-job',
        database = target_database,
        schema = target_schema,
        connection = glue_connection,
        definitions = f'{datalake_database_name}/{target_schema}.yaml'
    )

    with open(f'output/cloudformation/jobs/{datalake_database_name}/child_{datalake_database_name}.yaml', mode = 'w') as f:
        f.write(dump(glue_jobs, sort_keys = False).replace("'", ""))