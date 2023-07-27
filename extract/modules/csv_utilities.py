from io import StringIO
import csv

def create_csv_writer():
    """
    Creates a buffer and csv writer
    \n
    Returns:
    --------
    Returns a `StringIO` object and `csv.writer` object.
    """
    buffer = StringIO()
    writer = csv.writer(buffer, delimiter = '|')
    return(buffer, writer)