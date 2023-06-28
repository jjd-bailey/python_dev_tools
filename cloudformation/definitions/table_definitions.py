from text_formatting.text_formatting import (
    camelcase_to_snakecase,
)


def create_table_definitions_dicts(
        tables: list,
        sql_file: str,
        camel_case: bool = True
    ):
    '''
        Create the definitions .yaml file for cloud formation for specified tables
        indicate if Table Names need to be converted to snake_case
    '''
    res = {}
    for i in tables:
        for n in i.keys():
            old_name = n

        if camel_case:
            new_name = camelcase_to_snakecase(old_name.split('.')[2])
        else:
            new_name = old_name.split('.')[2].lower()

        res[new_name] = {
            'sql': sql_file,
            'parameters': {
                'database': i[old_name]['database'],
                'schema': i[old_name]['schema'],
                'table': i[old_name]['table'],
                'grain': i[old_name]['primary_column'],
                'predicate_column1': '__NoColumnNameDefined__',
                'predicate_column2': '__NoColumnNameDefined__'
            }
        }
    return(res)