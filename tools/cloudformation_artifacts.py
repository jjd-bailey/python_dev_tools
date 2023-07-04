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

def create_glue_job_artifacts(
        server_name: str,
        port_number: int,
        target_database: str,
        glue_connection: str,
        sql_file_name: str,
        job_type: str,
):
    #  'sdh-operational-full-grain-extract'
    glue_job_name = f"{target_database.replace('_','-').lower()}-{job_type.replace('_','-').lower()}"

    mssql_engine = create_mssql_engine(
        host = server_name,
        port = port_number,
        database = target_database
    )

    database_tables = get_mssql_tables(engine = mssql_engine)

    create_table_definitions_dicts(
        tables = database_tables,
        sql_file = sql_file_name,
        database_name = target_database,
        extract_type = job_type,
        camel_case = True
    )

    table_groups = group_tables(
        tables = database_tables,
        chunk_size = 1e5,
        chunk_time_s = 5,
        group_time_m = 20,
        camel_case = True
    )

    create_glue_pyshell_jobs_dicts(
        schema_group = table_groups,
        job_name = glue_job_name,
        extract_type = job_type,
        connection = glue_connection,
    )