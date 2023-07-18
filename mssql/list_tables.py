'''
    pip install sqlalchemy==1.4.36
    pip install pymssql==2.2.7
'''

from sqlalchemy import (
    text,
    create_engine,
)
from sqlalchemy.engine import (
    URL,
)

def create_mssql_engine(
        host: str,
        port: int,
        database: str,
        username: str = None,
        password: str = None
    ):
    """
    Create an engine for MSSQL Servers
    """
    uri = URL.create(
        drivername = 'mssql+pymssql',
        host = host,
        database = database,
        port = port,
        username = username,
        password = password
    )
    engine = create_engine(uri)

    return(engine)

def get_mssql_tables(
        engine,
    ) -> list[dict]:
    """
    Get all tables from MSSQL databases for a specified schema/s
    Include meta data: Number of Rows, Size of Table, Unit to measure the size
    """
    list_tables = text("""
            WITH cteCreated AS
            (
            SELECT
                CreateDate.object_id
                ,CreateDate.name
            FROM sys.columns CreateDate WITH (NOLOCK)
            INNER JOIN sys.types o WITH (NOLOCK)
                ON  CreateDate.system_type_id = o.system_type_id
                AND o.name IN ('datetime2','datetime','timestamp')
            WHERE
                CreateDate.name LIKE '%create%'
            ), cteUpdated AS
            (
            SELECT
                CreateDate.object_id
                ,CreateDate.name
            FROM sys.columns CreateDate WITH (NOLOCK)
            INNER JOIN sys.types o WITH (NOLOCK)
                ON  CreateDate.system_type_id = o.system_type_id
                AND o.name IN ('datetime2','datetime','timestamp')
            WHERE
                CreateDate.name LIKE '%update%'
            AND CreateDate.name != 'FollowUpDate'
            )
            SELECT DISTINCT
                DB_NAME()                                                          AS DatabaseName
                ,q.name                                                             AS SchemaName
                ,w.name                                                             AS TableName
                ,ISNULL(e.name,'__NoColumnNameDefined__')                           AS ColumnName
                ,r.name                                                             AS ColumnDataType
                ,y.rows                                                             AS RecordCount
                ,SUM(u.used_pages) * 8                                              AS UsedSpaceKB
                ,IIF(y.rows = 0,0,CAST(SUM(u.used_pages) * 8 AS FLOAT) / y.rows)    AS KbPerRow
                ,ISNULL(i.name,'__NoColumnNameDefined__')                           AS CreateDelta
                ,COALESCE(o.name,i.name,'__NoColumnNameDefined__')                  AS UpdateDelta
            FROM sys.schemas q WITH (NOLOCK)
            INNER JOIN sys.tables w WITH (NOLOCK)
                ON  q.schema_id = w.schema_id
            LEFT JOIN sys.columns e WITH (NOLOCK)
                ON  w.object_id = e.object_id
                AND e.is_identity = 1
            LEFT JOIN sys.types r WITH (NOLOCK)
                ON  e.system_type_id = r.system_type_id
            LEFT JOIN sys.indexes t WITH (NOLOCK)
                ON  w.object_id = t.object_id
            LEFT JOIN sys.partitions y WITH (NOLOCK)
                ON  w.object_id = y.object_id
                AND t.index_id = y.index_id
            LEFT JOIN sys.allocation_units u WITH (NOLOCK)
                ON  y.partition_id = u.container_id
            LEFT JOIN cteCreated i
                ON  w.object_id = i.object_id
            LEFT JOIN cteUpdated o
                ON  w.object_id = o.object_id
            WHERE
                w.type_desc = 'USER_TABLE'
            AND t.filter_definition IS NULL
            GROUP BY
                q.name
                ,w.name
                ,e.name
                ,r.name
                ,y.rows
                ,ISNULL(i.name,'__NoColumnNameDefined__')
                ,COALESCE(o.name,i.name,'__NoColumnNameDefined__')
            ;
    """)

    tables = []
    with engine.connect() as con:
        res = con.execute(
            statement = list_tables,
        )
        res = res.fetchall()

        for record in res:
            record_dict = {}
            record_dict['database'] = record[0]
            record_dict['schema'] = record[1]
            record_dict['table'] = record[2]
            record_dict['primary_column'] = record[3]
            record_dict['primary_column_data_type'] = record[4]
            record_dict['record_count'] = record[5]
            record_dict['used_space_kb'] = record[6]
            record_dict['kb_per_row'] = record[7]
            record_dict['create_delta'] = record[8]
            record_dict['update_delta'] = record[9]

            table_dict = {}
            table_key = ''
            table_key = f'{record[0]}.{record[1]}.{record[2]}'
            table_dict[table_key] = record_dict

            tables.append(table_dict)


    return(tables)