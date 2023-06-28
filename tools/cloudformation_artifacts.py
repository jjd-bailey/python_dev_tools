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
def check_directories(definitions_directory: str, template_directory: str):
    if not path.exists(f'zz_output/cloudformation/definitions/{definitions_directory}'):
        makedirs(f'zz_output/cloudformation/definitions/{definitions_directory}')


    if not path.exists(f'zz_output/cloudformation/jobs/{template_directory}'):
        makedirs(f'zz_output/cloudformation/jobs/{template_directory}')


def create_glue_job_artifacts(
        server_name: str,
        port_number: int,
        target_database: str,
        target_schema: str,
        glue_connection: str,
        datalake_database_name: str,
        glue_job_name: str,
        sql_file_name: str,
        definitions_directory: str,
        template_directory: str,
        job_type: str,
):


    check_directories(definitions_directory,template_directory)



    mssql_engine = create_mssql_engine(
        host = server_name,
        port = port_number,
        database = target_database
    )

    database_tables = get_mssql_tables(engine = mssql_engine)


    table_defs = create_table_definitions_dicts(
        tables = database_tables,
        sql_file = sql_file_name,
        camel_case = True
    )

    with open(f'zz_output/cloudformation/definitions/{definitions_directory}/{job_type}.yaml', mode = 'w') as f:
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
        definitions = f'{datalake_database_name}/{job_type}.yaml'
    )

    with open(f'zz_output/cloudformation/jobs/{template_directory}/child_{job_type}_{datalake_database_name}.yaml', mode = 'w') as f:
        f.write(dump(glue_jobs, sort_keys = False).replace("'", ""))