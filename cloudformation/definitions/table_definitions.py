from text_formatting.text_formatting import (
    camelcase_to_snakecase,
)
from os import (
    path,
    makedirs,
)
from yaml import (
    dump,
)


def create_table_definitions_dicts(
        tables: list,
        sql_file: str,
        database_name: str,
        extract_type: str,
        camel_case: bool = True,
    ):
    '''
        Create the definitions .yaml file for cloud formation for specified tables
        indicate if Table Names need to be converted to snake_case
    '''

    resource = {}
    schema_list = {}
    for table_data in tables:
        for table_name in table_data:
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
            'sql': sql_file,
            'parameters': {
                'database': table_data[table_name]['database'],
                'schema': table_data[table_name]['schema'],
                'table': table_data[table_name]['table'],
                'grain': table_data[table_name]['primary_column'],
                'predicate_column1': '__NoColumnNameDefined__',
                'predicate_column2': '__NoColumnNameDefined__'
            }
        }


    for schema in schema_list:
        body = {}
        for table in schema_list[schema]:
            body[table] = resource[table]


        ###
        # Check if directory exists
        ###
        if not path.exists(f'zz_output/cloudformation/definitions/{database_name.lower()}/{schema.lower()}'):
            makedirs(f'zz_output/cloudformation/definitions/{database_name.lower()}/{schema.lower()}')


        ###
        # Write config file
        ###
        with open(f'zz_output/cloudformation/definitions/{database_name.lower()}/{schema.lower()}/{extract_type.lower()}.yaml', mode = 'w') as f:
            f.write(dump(body, sort_keys = False))