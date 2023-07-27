import boto3
from botocore.exceptions import ClientError
import json
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Row, Engine, URL

from re import sub, split
from typing import Union
from decimal import Decimal
from io import StringIO
import sys
from math import log10, floor

def create_sql_engine(name):
    """
    Create SQL Engine from AWS Glue Connection
    \n
    Parameters:
    -----------
    name : str
        The name of an AWS Glue Connection.
    \n
    Returns:
    --------
    Retruns a `sqlalchemy.engine.Engine object`.
    """
    glue = boto3.client('glue')
    connection = glue.get_connection(Name = name)
    properties = connection['Connection']['ConnectionProperties']
    username = properties['USERNAME']
    password = properties['PASSWORD']
    jdbc = properties['JDBC_CONNECTION_URL']
    jdbc_parts = split('/|:|;|=', jdbc)
    dialect = jdbc_parts[1]
    dialect = 'mssql' if dialect == 'sqlserver' else dialect
    if dialect == 'mssql':
        db_driver = 'pymssql'
    elif dialect == 'postgresql':
        db_driver = 'psycopg2'
    elif dialect == 'mysql':
        db_driver = 'pymysql'
    else:
        raise Exception(f"No driver available for the SQL dialect {dialect}.")
    host = jdbc_parts[4]
    port = jdbc_parts[5]
    database = jdbc_parts[-1]
    url = URL.create(
        drivername = f'{dialect}+{db_driver}',
        host = host,
        port = port,
        username = username,
        password = password,
        database = database
    )
    engine = create_engine(url)
    return(engine)

def format_sql_row(x: Union[list, Row], pop: int) -> list:
    """
    Formats a `list` or `sqlalchemy.engine.Row` object for writing to csv
    \n
    Parameters:
    -----------
    x : list | sqlalchemy.engine.Row
        The object to format.
    pop : int
        Remove the item the index specified in the `pop` argument.
    \n
    Returns:
    --------
    Retruns a `list` formatted for csv writing.
    """
    x = [float(i) if isinstance(i, Decimal) else i for i in x]
    x.pop(pop) if len(x) > pop else x
    return(x)


def test_chunk_size(
        buffer: StringIO,
        n_rows: int,
        max_rows: int,
        max_size: int
    ) -> int:
    """
    Test the table extract chunk size
    \n
    Parameters:
    -----------
    buffer : StringIO
        The `StringIO` object (chunk) to test.
    n_rows : int
        The number of rows in the `buffer` object.
    max_rows : int
        The maximum desired rows of a chunk.
    max_size : int
        The maximum desired size (Mb) of a chunk.
    \n
    Returns:
    --------
    Retruns a suggested chunk row count.
    """
    if n_rows < max_rows:
        test_size = sys.getsizeof(buffer.getvalue()) / 1e6
        row_size = test_size / n_rows
        rows_in_max = max_size / row_size
        if rows_in_max - max_rows < 0:
            mag = 10**floor(log10(rows_in_max))
            max_rows = (rows_in_max // mag) * mag
            max_rows = n_rows if max_rows < n_rows else max_rows
        return(int(max_rows))
    else:
        return(int(n_rows))



class Bookmark:
    def __init__(self, bucket: str, key: str) -> None:
        self.bucket = bucket
        self.key = key

    def get(self):
        try:
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket = self.bucket, Key = self.key)
            bookmark = json.loads(response['Body'].read().decode('utf-8'))
            value = bookmark['bookmark']
        except ClientError as error:
            if error.response['Error']['Code'] == 'NoSuchKey':
                value = None
            else:
                raise error
        except Exception as error:
            raise error
        return(value)

    def put(self, value) -> str:
        if value is not None:
            value = str(value)
            body = json.dumps({'bookmark': value})
            s3 = boto3.client('s3')
            response = s3.put_object(
                Bucket = self.bucket,
                Key = self.key,
                Body = body
            )
        return(value)