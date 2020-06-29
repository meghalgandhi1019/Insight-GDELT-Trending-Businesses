import zipfile
import urllib3
import re
import requests
import boto3
from io import BytesIO
import sys
from datetime import datetime

def upload_s3(bucket_address, start_date):
    '''
    This function is used to upload all the files which dated after start date(year-month) directly to S3 bucket using GDELT api.
    :param bucket_address: Base address of the S3 bucket.
    :param start_date: year-month used as a reference to upload.
    :return: Nothing returned. Files will be uploaded successfully.
    '''

    # create a http pool
    http = urllib3.PoolManager()

    # get the list of files
    raw_file_list = http.request('GET', 'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt')

    base_url = 'http://data.gdeltproject.org/gdeltv2/'
    url_lines = (raw_file_list.data.decode()).split('\n')

    session = boto3.Session()
    s3 = session.resource('s3')

    main_bucket = s3.Bucket(bucket_address)

    events_folder = 'v2_events_2019_onwards/'
    mentions_folder = 'mentions_2019_onwards/'

    for line in url_lines[0:-1]:  # Remove last element as It's blank space only.
        filename = re.compile(r'(\d+)\D*$').search(line).group(1)
        year_month = int(filename[0:6]) # YYYYMM


        if year_month >= start_date and 'export' in line:

            r = requests.get(base_url + filename + '.export.CSV.zip')
            filebytes = BytesIO(r.content)
            event_zipfiles = zipfile.ZipFile(filebytes)
            main_bucket.upload_fileobj(event_zipfiles.open(filename + '.export.CSV'), events_folder + filename + '.export.CSV')
            print(filename + '.export.CSV', ' uploaded to s3 successfully')

        if year_month >= start_date and 'mentions' in line:
            r = requests.get(base_url + filename + '.mentions.CSV.zip')
            filebytes = BytesIO(r.content)
            mentions_zipfiles = zipfile.ZipFile(filebytes)
            main_bucket.upload_fileobj(mentions_zipfiles.open(filename + '.mentions.CSV'), mentions_folder + filename+'.mentions.CSV')
            print(filename + '.mentions.CSV', ' uploaded to s3 successfully')
    return

if __name__ == "__main__":

    # Get the year and month from command line arguement
    cli_date = datetime.strptime(sys.argv[1], '%Y-%m-%d %H:%M:%S')
    year = str(cli_date.year)
    month = str(cli_date.month)
    year_month_requested = year + month

    upload_s3('gdelt-2019-onwards', year_month_requested)

