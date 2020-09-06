#!/usr/bin/env python

# Imports
import pandas as pd
import pycamhd as camhd
from datetime import datetime
import yaml
from azure.storage.blob import BlobServiceClient
import fsspec

# constants
repodir = '/home/tjc/github/ooicloud/ooi-opendata/'

# Open current dbcamhd.json file
dbcamhd_url = 'https://ooiopendata.blob.core.windows.net/camhd/dbcamhd.json'
with fsspec.open(dbcamhd_url) as f:
    dbcamhd = pd.read_json(f, orient="records", lines=True, convert_dates = False, dtype=False)


# Get list of files on Azure
with open(repodir + 'secrets/tjcrone.yml', 'r') as stream:
    keys = yaml.safe_load(stream)
storage_account_url = 'https://ooiopendata.blob.core.windows.net'
blob_service_client = BlobServiceClient(storage_account_url, credential = keys['camhd'])
container_client = blob_service_client.get_container_client('camhd')
blob_iter = container_client.list_blobs()


# Filter blobs already in database
blob_list = []
i = 0
for blob in blob_iter:
    i = i+1
    if blob.name not in dbcamhd['name'].values and blob.name.endswith('mov'):
        blob_list.append(blob)


# Deployment function
def get_deployment(timestamp):
    dt = datetime.fromtimestamp(timestamp)
    if dt < datetime(2016,7,26,21,18,0):
        return 2
    elif dt >= datetime(2016,7,26,21,18,0) and dt < datetime(2017,8,14,6,0,0):
        return 3
    elif dt >= datetime(2017,8,14,6,0,0) and dt < datetime(2018,7,4,0,0,0):
        return 4
    elif dt >= datetime(2018,7,4,0,0,0) and dt < datetime(2019,6,16,2,0,0):
        return 5
    elif dt >= datetime(2019,6,16,2,0,0) and dt < datetime(2020,8,6,11,0,0):
        return 6
    elif dt >= datetime(2020,8,6,11,0,0):
        return 7
    else:
        return None


# Database entry function
def get_entry(blob):
    name = blob.name
    url = 'https://ooiopendata.blob.core.windows.net/camhd/' + blob.name
    filesize = blob.size
    md5_64 = blob['content_settings']['content_md5']
    if md5_64:
        md5 = blob['content_settings']['content_md5'].hex()
    else:
        md5 = 0

    try:
        moov_atom = camhd.get_moov_atom(url)
        moov = True
        timestamp = camhd.get_timestamp(url, moov_atom)
        deployment = get_deployment(timestamp)
        frame_count = camhd.get_frame_count(url, moov_atom)
    except:
        moov = False
        timestamp = 0
        deployment = 0
        frame_count = 0
        
    return pd.DataFrame([[name, url, filesize, md5, moov, timestamp, deployment, frame_count]], 
                        columns = ['name', 'url', 'filesize', 'md5', 'moov', 'timestamp',
                                   'deployment', 'frame_count'])


# Append to dbcamhd.json
for blob in blob_list:
    dbcamhd = pd.concat([dbcamhd, get_entry(blob)])
dbcamhd = dbcamhd.reset_index(drop = True)


# Save to Azure
dbcamhd.to_json('dbcamhd.json', orient="records", lines=True)
blob_client = blob_service_client.get_blob_client(container = 'camhd', blob = 'dbcamhd.json')
with open('dbcamhd.json', 'rb') as data:
    blob_client.upload_blob(data, overwrite = True)

dbcamhd.to_csv('dbcamhd.csv')
blob_client = blob_service_client.get_blob_client(container = 'camhd', blob = 'dbcamhd.csv')
with open('dbcamhd.csv', 'rb') as data:
    blob_client.upload_blob(data, overwrite = True)
