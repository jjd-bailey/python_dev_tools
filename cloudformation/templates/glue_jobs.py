from text_formatting.text_formatting import (
    camelcase_to_snakecase,
    snakecase_to_camelcase,
)
from math import (
    ceil,
    floor,
)
from os import (
    path,
    makedirs,
)
from yaml import (
    dump,
)


def group_tables(
        tables: list,
        chunk_size: int = 100000,
        chunk_time_s: int = 8,
        group_time_m: int = 20,
        camel_case: bool = True
    ):
    table_sorted = sorted(
        tables,
        key = lambda x: (list(x.values())[0]['schema'],list(x.values())[0]['record_count']),
        reverse = True
    )

    # Schema groups for the loop
    build_group = []
    old_schema = 'default_schema_to_be_overwritten'
    schema_group = {}

    # Chunk groups for the loop
    max_chunks = floor((group_time_m * 60) / chunk_time_s)
    current_chunks = 0


    for table_data in table_sorted:
        table_key = list(table_data.keys())[0]

        ####
        # Administration
        ####
        if old_schema == 'default_schema_to_be_overwritten':
            old_schema = f"{table_data[table_key]['database'].lower()}.{table_data[table_key]['schema'].lower()}"
        schema = f"{table_data[table_key]['database'].lower()}.{table_data[table_key]['schema'].lower()}"

        if camel_case:
            new_name = camelcase_to_snakecase(table_data[table_key]['table'])
        else:
            new_name = table_data[table_key]['table'].lower()

        chunks = ceil(table_data[table_key]['record_count'] / chunk_size)
        current_chunks += chunks

        ####
        # The Magic!!
        ####
        if schema != old_schema:
            #####
            #   If the schema has changed we need to start a new group
            #       Write the built group to the schema Dict and start building a new group
            #####
            if old_schema not in schema_group:
                ####
                # Schema does not exist in dict -- create an empty list to append groups to
                ####
                schema_group[old_schema] = []
                schema_group[old_schema].append(build_group)
            else:
                schema_group[old_schema].append(build_group)
            build_group = []
            build_group.append(new_name)
            old_schema = schema
        else:
            #####
            #   Continue building the existing schema's group of tables
            #####
            if current_chunks >= max_chunks:
                ####
                # If the groups chunk exceeds the max, write the group to the schema dict
                # Start the group from clean list
                ####
                if schema not in schema_group:
                    ####
                    # Schema does not exist in dict -- create an empty list to append groups to
                    ####
                    schema_group[old_schema] = []
                    schema_group[old_schema].append(build_group)
                else:
                    schema_group[old_schema].append(build_group)

                build_group = []
                build_group.append(new_name)
                old_schema = schema
                current_chunks = 0
            else:
                build_group.append(new_name)
                old_schema = schema

    ####
    # Once the for loop is completed, check if there's a group that was built:
    #   Write it to the schema dict
    ####
    if len(build_group) > 0:
        if schema not in schema_group:
            schema_group[old_schema] = []
            schema_group[old_schema].append(build_group)
        else:
            schema_group[schema].append(build_group)
        build_group = []


    return(schema_group)


def create_glue_pyshell_jobs_dicts(
        schema_group: list,
        job_name: str,
        extract_type: str,
        connection: str,
    ):
    resources = {}

    for database_schema in schema_group:
        ###
        # Job variables
        ###
        database = database_schema.split('.')[0]
        schema = database_schema.split('.')[1]
        definitions = f'{database}/{schema}/{extract_type}.yaml'
        glue_job_name = f'{job_name}-{schema}'

        for ordinal_position, schema_tables in enumerate(schema_group[database_schema]):
            n = ordinal_position + 1

            resources[f'{snakecase_to_camelcase(glue_job_name, "-")}{n}'] = {
                'Type': 'AWS::Glue::Job',
                'Properties': {
                    'Name': f'!Sub ${{AWS::StackName}}-{glue_job_name}-{n}',
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
                        '--job_name': f'!Sub ${{AWS::StackName}}-{glue_job_name}-{n}',
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
                        '--tables': ', '.join(schema_tables)
                    },
                    'ExecutionProperty': {'MaxConcurrentRuns': 1},
                    'MaxCapacity': 0.0625,
                    'MaxRetries': 0,
                    'Timeout': 2880,
                }
            }


    jobs = []
    for job in resources.keys():
        jobs.append(f'JobName: !Ref {job}')

    template = {
        'AWSTemplateFormatVersion': '2010-09-09',
        'Description': '> \
Creates the necessary infrastructure to extract the SDH Operational tables.',

        'Parameters': {
            'CodeBucket': {
                'Type': 'String',
                'Description': 'The name of the s3 bucket where the code is located.',
                'AllowedPattern': '^[a-z][a-z0-9-]*$',
                'ConstraintDescription': 'Should be a valid s3 bucket name.'
            }
        },
        'CodeRepo': {
            'Type': 'String',
            'Description': '> \
The repository name containing the code.',
            'AllowedPattern': '^[a-zA-Z][a-zA-Z0-9_.-/]*[a-zA-Z0-9_\.-]$',
            'ConstraintDescription': '> \
Should start with a letter and can only contain alphanumeric characters \
and the special characters (_.-).'
        },
        'DataBucket': {
            'Type': 'String',
            'Description': 'The name of the s3 bucket used to store extract results.',
            'AllowedPattern': '^[a-z][a-z0-9-]*$',
            'ConstraintDescription': 'Should be a valid s3 bucket name.'
        },
        'GlueJobRole': {
            'Type': 'String',
            'Description': 'The ARN of a IAM Role for Glue Jobs to assume.'
        },
        'SnsFailureTopic': {
            'Type': 'String',
            'Description': 'The ARN of a topic where to post Glue Job failures'
        },
        connection: {
            'Type': 'String',
            'Description': f'> \
The name of the Glue Connection connecting to the {database} database.'
        },

        'Resources': resources,
        f'{snakecase_to_camelcase(glue_job_name, "-")}ManualTrigger': {
            'Type': 'AWS::Glue::Trigger',
            'Properties': {
                'Name': f'!Sub ${{AWS::StackName}}-{glue_job_name}-manual-trigger',
                'Type': 'ON_DEMAND',
                'Description': 'call this trigger manually when the job needs to be run',
                'Actions': jobs
            }
        },
        f'{snakecase_to_camelcase(glue_job_name, "-")}Daily2300Trigger': {
            'Type': 'AWS::Glue::Trigger',
            'Properties': {
                'Name': f'!Sub ${{AWS::StackName}}-{glue_job_name}-daily-2300-trigger',
                'Type': 'SCHEDULED',
                'Description': 'Schedule',
                'Schedule': 'cron(0 23 * * ? *)',
                'StartOnCreation': 'false',
                'Actions': jobs
            }
        }
    }

    ###
    # Check if directory exists
    ###
    if not path.exists(f'zz_output/cloudformation/jobs/{database}/{extract_type.lower()}'):
        makedirs(f'zz_output/cloudformation/jobs/{database}/{extract_type.lower()}')


    ###
    # Write config file
    ###
    with open(f'zz_output/cloudformation/jobs/{database}/{extract_type.lower()}/child_{database}_{extract_type.lower()}.yaml', mode = 'w') as f:
        f.write(dump(template, sort_keys = False))