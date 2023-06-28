from text_formatting.text_formatting import (
    camelcase_to_snakecase,
    snakecase_to_camelcase,
)
from math import (
    ceil,
    floor,
)


def group_tables(
        tables: list,
        chunk_size: int = 100000,
        chunk_time_s: int = 8,
        group_time_m: int = 20,
        camel_case: bool = True
    ):
    tables = sorted(
        tables,
        key = lambda x: x['rows'],
        reverse = True
    )
    max_chunks = floor((group_time_m * 60) / chunk_time_s)
    groups = []
    in_group = []
    chunk_cs = 0
    for i in tables:
        name = i['name'].split('.')[2]
        if camel_case:
            name = camelcase_to_snakecase(name)
        else:
            name = name.lower()
        chunks =  ceil(i['rows'] / chunk_size)
        old_chunks_cs = chunk_cs
        chunk_cs += chunks
        if chunk_cs >= max_chunks:
            if old_chunks_cs != 0:
                groups.append(in_group)
                in_group = []
                in_group.append(name)
                chunk_cs = 0
            else:
                in_group.append(name)
                groups.append(in_group)
                in_group = []
                chunk_cs = 0
        else:
            in_group.append(name)
    if chunk_cs != 0:
        groups.append(in_group)
    return(groups)


def create_glue_pyshell_jobs_dicts(
        groups: list,
        job_name: str,
        database: str,
        schema: str,
        connection: str,
        definitions: str
    ):
    res = {}
    for i, g in enumerate(groups):
        n = i + 1
        res[f'{snakecase_to_camelcase(job_name, "-")}{n}'] = {
            'Type': 'AWS::Glue::Job',
            'Properties': {
                'Name': f'!Sub ${{AWS::StackName}}-{job_name}-{n}',
                'Description': ''.join([
                    f"Extracts tables defined in the --tables argument below ",
                    f"from the {schema} schema in the {database} SQL Server ",
                    "database."
                ]),
                'Role': '!Ref GlueJobRole',
                'Command': {
                    'Name': 'pythonshell',
                    'PythonVersion': '3.9',
                    'ScriptLocation': '!Sub s3://${CodeBucket}/${CodeRepo}/scripts/glue_pyshell_extract.py'
                },
                'Connections': {'Connections': [f'!Ref {connection}']},
                'DefaultArguments': {
                    'library-set': 'analytics',
                    '--job_name': f'!Sub ${{AWS::StackName}}-{job_name}-{n}',
                    '--connection_name': f'!Ref {connection}',
                    '--code_bucket': '!Ref CodeBucket',
                    '--code_repo': '!Ref CodeRepo',
                    '--data_bucket': '!Ref DataBucket',
                    '--definitions': definitions,
                    '--sns_failure_topic': '!Ref SnsFailureTopic',
                    '--additional-python-modules': 'pymssql',
                    '--TempDir': '!Sub s3://${CodeBucket}/${CodeRepo}/GLUE-TEMPORARY/',
                    '--max_rows_per_chunk': 100000,
                    '--max_memory_per_chunk': 100,
                    '--sql_stream_yield': 5000,
                    '--tables': ', '.join(g)
                },
                'ExecutionProperty': {'MaxConcurrentRuns': 1},
                'MaxCapacity': 0.0625,
                'MaxRetries': 0,
                'Timeout': 2880,
            }
        }
    return(res)