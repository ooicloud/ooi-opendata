#!/usr/bin/env python

# imports
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
import yaml
import subprocess
import datetime

# get list of files on raw data server
def get_raw_list(days=None):
    """
    Return a list of files from the Raw Data Server.

    Parameters
    ----------
    days : int, optional
        Number of days from today to look back in time.

    Returns
    -------
    list : asdf
    """

    days=0
    if days is None:
        start = datetime.date(2015, 6, 1)
    else:
        start = datetime.date.today() - datetime.timedelta(days=days)
    dates = pd.date_range(start=start, end=datetime.date.today())

    raw_list = []
    for date in dates.date:
        url = 'https://rawdata.oceanobservatories.org/files/RS03ASHS/PN03B/06-CAMHDA301/%i/%02.0f/%02.0f/' % (date.year, date.month, date.day)
        response = requests.get(url)
        if response.ok:
            soup = BeautifulSoup(response.text, 'html.parser')
            raw_list = raw_list + [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith('mov')]
    return raw_list


# get list of files on Azure
def get_ooiopendata_list(container=None, sas_token=None):
    storage_account_url = 'https://ooiopendata.blob.core.windows.net'
    blob_service_client = BlobServiceClient(storage_account_url, credential = sas_token)
    container_client = blob_service_client.get_container_client(container)
    ooiopendata_list = [blob.name for blob in container_client.list_blobs()]
    return ooiopendata_list


# filter list
def get_transfer_list(raw_list, ooiopendata_list):
    transfer_list = []
    for url in raw_list:
        filename = url.split('/')[-1].strip()
        if filename in azure_list:
            blob_client = blob_service_client.get_blob_client(container = 'camhd', blob = filename)
            md5_hash = blob_client.get_blob_properties()['content_settings']['content_md5']
            if not md5_hash:
                transfer_list.append(url)
        if filename not in azure_list:
            transfer_list.append(url)
    return transfer_list


# transfer files
def transfer_files(transfer_list, max_file_size=None):
    container = 'https://ooiopendata.blob.core.windows.net/camhd?'
    for i, url in enumerate(transfer_list):
        filename = url.split('/')[-1].strip()
        size = int(requests.get(url, stream=True).headers['Content-length'])/1024/1024/1024
        if size > max_file_size:
            print('%s  Skipping %s (%f GB)' % (datetime.datetime.now(), filename, size))
        else:
            print('%s  Copying %s [%i/%i]' % (datetime.datetime.now(), filename, i+1, len(transfer_list)))
#            subprocess.check_output(['wget', '-q', '-O', '/mnt/opendata/%s' % filename, url])
#            subprocess.check_output(['/usr/local/bin/azcopy', 'copy', '/mnt/opendata/%s' % filename, container + keys['camhd'], '--put-md5'])
#            subprocess.check_output(['rm', '/mnt/opendata/%s' % filename])


# delete files on Azure for testing
#for file_url in files_to_transfer:
#    filename = file_url.split('/')[-1].strip()
#    print('Deleting %s' % filename)
#    blob_client = blob_service_client.get_blob_client(container = 'camhd', blob = filename)
#    blob_client.delete_blob()


def main():
    raw_list = get_raw_list(0)
    print(raw_list)


    #with open(repodir + 'secrets/tjcrone.yml', 'r') as stream:
    #    keys = yaml.safe_load(stream)

    #sas_token = keys['camhd']

    #print(sas_token)

# constants
#repodir = '/home/tjc/github/ooicloud/ooi-opendata/'

# output log message
#print('Log message ...')

if __name__ == '__main__':
    main()
