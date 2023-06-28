from re import (
    sub
)



def camelcase_to_snakecase(string: str) -> str:
    """
    Convert CamelCase to snake_case
    """

    res = sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    res = sub(' |,', '_', res)
    res = sub('([a-z0-9])([A-Z])', r'\1_\2', res).lower()
    res = sub('([a-z])([0-9])', r'\1_\2', res)
    res = sub('__', '_', res)

    return(res)



def snakecase_to_camelcase(string: str, split = '_') -> str:
    """
    Convert snake_case to CamelCase
    """
    return(''.join(word.title() for word in string.split(split)))