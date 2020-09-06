#!/usr/bin/env python

# Imports
import numpy as np
import requests
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
import yaml
import subprocess
from datetime import datetime
from datetime import timedelta

# Constants
repodir = '/home/tjc/github/ooicloud/ooi-opendata/'

# Get list of files on raw data server
def get_raw_list():
    dates = []
    for i in range(4): # scrape index files from three days ago to present
       dates.append(datetime.now() - timedelta(days=i))

    ext = 'mov'
    filelist = []

    for date in dates:
        url = 'https://rawdata.oceanobservatories.org/files/RS03ASHS/PN03B/06-CAMHDA301/%i/%02.0f/%02.0f/' % (date.year, date.month, date.day)
        response = requests.get(url)
        if response.ok:
            soup = BeautifulSoup(response.text, 'html.parser')
            filelist = filelist + [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]

    return filelist

raw_list = get_raw_list()

# Get list of files on Azure
with open(repodir + 'secrets/tjcrone.yml', 'r') as stream:
    keys = yaml.safe_load(stream)
storage_account_url = 'https://ooiopendata.blob.core.windows.net'
blob_service_client = BlobServiceClient(storage_account_url, credential = keys['camhd'])
container_client = blob_service_client.get_container_client('camhd')
azure_list = [blob.name for blob in container_client.list_blobs()]


# Filter list
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


# transfer files
container = 'https://ooiopendata.blob.core.windows.net/camhd?'
for i, url in enumerate(transfer_list):
    filename = url.split('/')[-1].strip()
    size = int(requests.get(url, stream=True).headers['Content-length'])/1024/1024/1024
    if size > 40:
        print('Skipping %s (%f GB)' % (filename, size))
    else:
        print('Copying %s [%i/%i]' % (filename, i+1, len(transfer_list)))
        subprocess.check_output(['wget', '-q', '-O', '/mnt/opendata/%s' % filename, url])
        subprocess.check_output(['/usr/local/bin/azcopy', 'copy', '/mnt/opendata/%s' % filename, container + keys['camhd'], '--put-md5'])
        subprocess.check_output(['rm', '/mnt/opendata/%s' % filename])


# delete files on Azure for testing
#for file_url in files_to_transfer:
#    filename = file_url.split('/')[-1].strip()
#    print('Deleting %s' % filename)
#    blob_client = blob_service_client.get_blob_client(container = 'camhd', blob = filename)
#    blob_client.delete_blob()


