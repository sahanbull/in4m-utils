import gzip

from StringIO import StringIO
from boto.s3.connection import S3Connection

from in4m_utils.general.url import parse_url


def get_data_from_s3(s3_url):
    """Gets data from a given s3 path
    Args:
        s3_url : s3 url to the file to be read

    Returns:
        data [str]: contents of the url in string lines

    """
    _, bucket, path, _, _, _ = parse_url(s3_url)
    key = _get_s3_key(bucket, path)
    data = [line for line in key.get_contents_as_string().split('\n')]
    return data


def _get_s3_key(bucket, path, conn=S3Connection()):
    """Get the s3 key
    Args:
        bucket(str): bucket name
        path (str): url path to the file
        conn (S3Connection): s3 connection to be used

    Returns:
        key (Key): key of the url
    """
    bucket = conn.get_bucket(bucket)
    key = bucket.get_key(path[1:])
    return key


def _get_gz_data(data, filepath):
    """generates the gzip string to be saved as gzip
    Args:
        data (str): the data to be gzipped
        filepath (str): path of the file

    Returns:
        (str): gzipped version of the data
    """
    data_obj = StringIO()
    filename = filepath.split("/")[-1].replace(".gz","")
    with gzip.GzipFile(filename=filename, mode='wb', fileobj=data_obj) as gzip_outfile:
        gzip_outfile.write(data)
    return data_obj.getvalue()


def write_data_to_s3(data, s3_url, is_gzip=False, conn = S3Connection()):
    """writes data to s3 location
    Args:
        data (str): data in string format
        s3_url (str): path of the s3 url
        is_gzip (bool): if the data should be gzipped or not
    """
    _, bucket, path, _, _, _ = parse_url(s3_url)
    if is_gzip and not path[-3] == ".gz":
        path += ".gz"
        data = _get_gz_data(data, path)

    bucket = conn.get_bucket(bucket)
    s3_key = bucket.get_key(path[1:])
    s3_key.set_contents_from_string(data)


def get_keys_from_s3(s3_folder_path):
    """gets the s3 urls of all the files in a s3 folder
    Args:
        s3_folder_path (str): url of the s3 folder path to look into
    Returns:
        all_keys ([str]): list of keys in the folder
    """
    if s3_folder_path[-1] != '/':
        s3_folder_path += '/'

    _, bucket, path, _, _, _ = parse_url(s3_folder_path)
    all_keys = set([])
    for key in bucket.list(prefix=path[1:]):
        filename = key.name.replace(path[1:],'')
        if s3_folder_path+str(filename).split('/')[0] == s3_folder_path:
            continue
        all_keys.add(s3_folder_path+str(filename).split('/')[0])
    return all_keys

