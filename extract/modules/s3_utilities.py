import boto3

def s3_read_yaml(bucket: str, key: str) -> Union[dict, list]:
    """
    Read YAML file on AWS s3
    \n
    Parameters:
    -----------
    bucket : str
        The AWS s3 bucket name.
    key : str
        The key to the YAML file. Should not contain a leading slash '/' and
        must end in '.yaml'. Eg., 'path/file.yaml'.
    \n
    Returns:
    --------
    Returns the contents of the YAML file as either a `dict` or `list`.
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket = bucket, Key = key)
    content = yaml.safe_load(response['Body'].read().decode('utf-8'))
    return(content)

def s3_read_json(bucket: str, key: str) -> Union[dict, list]:
    """
    Read JSON file on AWS s3
    \n
    Parameters:
    -----------
    bucket : str
        The AWS s3 bucket name.
    key : str
        The key to the JSON file. Should not contain a leading slash '/' and
        must end in '.json'. Eg., 'path/file.json'.
    \n
    Returns:
    --------
    Returns the contents of the JSON file as either a `dict` or `list`.
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket = bucket, Key = key)
    content = json.loads(response['Body'].read().decode('utf-8'))
    return(content)

def s3_read_sql(bucket: str, key: str) -> str:
    """
    Read SQL file on AWS s3
    \n
    Parameters:
    -----------
    bucket : str
        The AWS s3 bucket name.
    key : str
        The key to the SQL file. Should not contain a leading slash '/' and
        must end in '.sql'. Eg., 'path/file.sql'.
    \n
    Returns:
    --------
    Returns the contents of the SQL file as a `str`.
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket = bucket, Key = key)
    query = response['Body'].read().decode()
    return(query)

def s3_delete_objects(bucket: str, key: str):
    """
    Delete AWS s3 objects
    \n
    When the full path to an object is provided only that objects will be
    deleted. Multiple objects can be deleted by specifying a key that ends at
    the root of all objects to be deleted, eg., when 'path/root' is passed to
    the `key` argument all objects with the 'path/root' prfix will be deleted.
    \n
    Parameters:
    -----------
    bucket : str
        The AWS s3 bucket name.
    key : str
        The key to the object(s) to be deleted.
    \n
    Returns:
    --------
    `None`.
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    objects = bucket.objects.filter(Prefix = key)
    objects = [{'Key': i.key} for i in objects]
    if len(objects):
        bucket.delete_objects(Delete = {'Objects': objects, 'Quiet': True})