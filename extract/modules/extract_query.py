from extract.modules.s3_utilities import (
    s3_delete_objects,
    s3_read_sql,
)
from extract.modules.csv_utilities import (
    create_csv_writer,
)
from extract.modules.glue_utilities import (
    format_sql_row,
    test_chunk_size,
)
import boto3
from typing import Union
from sqlalchemy import text
from sqlalchemy.engine import Engine

class QueryExtract:
    def __init__(
            self,
            name: str,
            bucket: str,
            key: str,
            has_bookmark: bool,
            bookmark: str,
            chunks: int,
            rows: int
        ):
        self.name = name,
        self.bucket = bucket
        self.key = key
        self.has_bookmark = has_bookmark
        self.bookmark = bookmark
        self.chunks = chunks
        self.rows = rows

    def delete(self):
        s3_delete_objects(self.bucket, self.key)

class Query:
    def __init__(
            self,
            name: str,
            bucket: str,
            key: str,
            parameters: Union[dict, None],
            bookmark: Union[str, None],
            engine: Engine
        ):
        params = {} if parameters is None else parameters
        try:
            query = s3_read_sql(bucket, key)
            try:
                query = query.format(**params)
            except Exception as e:
                raise Exception("Not all query parameters were defined.")
            query = text(query)
            if 'BOOKMARK' in query.compile(engine).params:
                has_bookmark = True
                query = query.bindparams(BOOKMARK = bookmark)
                warning = None
            else:
                has_bookmark = False
                if bookmark is not None:
                    warning = ''.join([
                        f"Bookmark was set to `{bookmark}`, however query does not ",
                        "contain the `:BOOKMARK` specification. Bookmark set ",
                        "to `None`."
                    ])
                    bookmark = None
                else:
                    warning = None
        except Exception as error:
            raise error
        else:
            self.name = name
            self.query = query
            self.parameters = parameters
            self.has_bookmark = has_bookmark
            self.bookmark = bookmark
            self.engine = engine
            self._warning = warning

    def show_query(self):
        print(str(self.query.compile(compile_kwargs={"literal_binds": True})))

    def extract(
            self,
            bucket: str,
            key: str,
            metadata: Union[dict, None] = None,
            chunk_size: int = 100000,
            chunk_memory: int = 90,
            stream_size: int = 5000,
            verbose = True,
            printer = print
        ) -> QueryExtract:
        try:
            s3 = boto3.client('s3')
            metadata = {} if metadata is None else metadata
            chunk_key = f'{key}/chunk_{{n}}.csv'
            chunk_test_at = chunk_size * 0.01
            result_empty = True
            chunk_n = 1
            bookmark = self.bookmark
            n = 0
            with self.engine.connect().execution_options(yield_per = stream_size) as con:
                result = con.execute(self.query)
                fields = list(result.keys())
                if self.has_bookmark:
                    bm_i = fields.index('__<NEWBOOKMARK>__')
                else:
                    bm_i = len(fields)
                buffer, writer = create_csv_writer()
                writer.writerow(fields)
                fields_str_len = len(buffer.getvalue())
                if verbose:
                    printer(f'Extracting chunk {chunk_n}')
                for i, row in enumerate(result):
                    n = i + 1
                    if n == 1:
                        result_empty = False
                        if self.has_bookmark:
                            bookmark = str(format_sql_row(row, len(fields))[bm_i])
                    if n % chunk_size:
                        writer.writerow(format_sql_row(row, bm_i))
                        if n == chunk_test_at:
                            chunk_size = test_chunk_size(
                                buffer = buffer,
                                n_rows = chunk_test_at,
                                max_rows = chunk_size,
                                max_size = chunk_memory
                            )
                            if n == chunk_size:
                                if verbose:
                                    printer(f'Writing chunk {chunk_n}')
                                write_response = s3.put_object(
                                    Bucket = bucket,
                                    Key = chunk_key.format(n = str(chunk_n).zfill(7)),
                                    Body = buffer.getvalue(),
                                    Metadata = metadata
                                )
                                chunk_n += 1
                                buffer, writer = create_csv_writer()
                    else:
                        writer.writerow(format_sql_row(row, bm_i))
                        if verbose:
                            printer(f'Writing chunk {chunk_n}')
                        write_response = s3.put_object(
                            Bucket = bucket,
                            Key = chunk_key.format(n = str(chunk_n).zfill(7)),
                            Body = buffer.getvalue(),
                            Metadata = metadata
                        )
                        chunk_n += 1
                        buffer, writer = create_csv_writer()
                        writer.writerow(fields)
                        if verbose:
                            printer(f'Extracting chunk {chunk_n}')
                if len(buffer.getvalue()) > fields_str_len:
                    if verbose:
                        printer(f'Writing chunk {chunk_n}')
                    write_response = s3.put_object(
                        Bucket = bucket,
                        Key = chunk_key.format(n = str(chunk_n).zfill(7)),
                        Body = buffer.getvalue(),
                        Metadata = metadata
                    )
                else:
                    if verbose:
                        printer('Nothing to extract')
                    chunk_n -= 1
            return(QueryExtract(
                name = self.name,
                bucket = bucket,
                key = key,
                has_bookmark = self.has_bookmark,
                bookmark = bookmark,
                chunks = 0 if result_empty else chunk_n,
                rows = n
            ))
        except Exception as error:
            error = ''.join([
                f"Failed to extract table: {error} ",
                f'Removing all chunks from s3//{bucket}/{key}.'
            ])
            try:
                s3_delete_objects(bucket, key)
                error = f'{error} Successfully removed all chunks.'
            except Exception as del_error:
                error = f'{error} Failed to remove all chunks. {del_error}'
            raise Exception(error)