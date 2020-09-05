#!/usr/bin/env python

# Imports
import numpy as np
import requests
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
import yaml
import subprocess


# Get list of files on raw data server
def get_raw_list(year):
    url_base_year = 'https://rawdata.oceanobservatories.org/files/RS03ASHS/PN03B/06-CAMHDA301/' + str(year)
    ext = 'mov'
    filelist = []
    for month in list(np.arange(1,13)):
        url_base_month = '%s/%02.0f' % (url_base_year, month)
        response = requests.get(url_base_month)
        if response.ok:
            for day in list(np.arange(1,31)):
                url_base_day = '%s/%02.0f/' % (url_base_month, day)
                response = requests.get(url_base_day)
                if response.ok:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    filelist = filelist + [url_base_day + 
                              node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
    return filelist

raw_list = get_raw_list(2020)


# Get list of files on Azure
with open('../secrets/tjcrone.yml', 'r') as stream:
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


