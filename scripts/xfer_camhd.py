#!/usr/bin/env python

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
import yaml
import subprocess
import datetime
import pycamhd as camhd
import fsspec


def get_raw_list(days=None):
    """
    Return a list of files from the Raw Data Server.

    Parameters
    ----------
    days : int, optional
        Number of days from today to look back in time. All files will be
        returned if no days argument is not passed.

    Returns
    -------
    list : str
    """
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


def get_ooiopendata_list(container=None, sas_token=None):
    """
    Return a list of file names from a container in the ooiopendata storage account.

    Parameters
    ----------
    container : str
        Name of container.
    sas_token : str
        SAS token for authenticated access.

    Returns
    -------
    list : str
    """
    ooiopendata_blobs = get_ooiopendata_blobs(container=container, sas_token=sas_token)
    ooiopendata_list = [blob.name for blob in ooiopendata_blobs]
    return ooiopendata_list


def get_ooiopendata_blobs(container=None, sas_token=None):
    """
    Return a list of blob properties from a container in the ooiopendata storage account.

    Parameters
    ----------
    container : str
        Name of container.
    sas_token : str
        SAS token for authenticated access.

    Returns
    -------
    list : Azure BlobProperties class.
    """
    storage_account_url = 'https://ooiopendata.blob.core.windows.net'
    blob_service_client = BlobServiceClient(storage_account_url, credential=sas_token)
    container_client = blob_service_client.get_container_client(container)
    ooiopendata_blobs = [blob for blob in container_client.list_blobs()]
    return ooiopendata_blobs


def get_transfer_list(raw_list, ooiopendata_list):
    """
    Return a list of files that need to be transfered to the ooiopendata storage account.

    Parameters
    ----------
    raw_list : list of str
        List of files on the Raw Data Server.
    ooiopendata_list : list of str
        List of files from the ooiopendata storage account.

    Returns
    -------
    list : str
    """
    storage_account_url = 'https://ooiopendata.blob.core.windows.net'
    blob_service_client = BlobServiceClient(storage_account_url)
    transfer_list = []
    for url in raw_list:
        filename = url.split('/')[-1].strip()
        if filename in ooiopendata_list:
            blob_client = blob_service_client.get_blob_client(container='camhd', blob=filename)
            md5_hash = blob_client.get_blob_properties()['content_settings']['content_md5']
            if not md5_hash:
                transfer_list.append(url)
        if filename not in ooiopendata_list:
            transfer_list.append(url)
    return transfer_list


def transfer_files(transfer_list, sas_token, max_file_size=None):
    """
    Transfer files from the Raw Data Server to Azure.

    Parameters
    ----------
    tranfer_list : list of str
        List of files to transfer.
    sas_token : str
        SAS token required for authenticated access.
    max_file_size : int, optional
        Maximum file size to transfer.
    """
    container = 'https://ooiopendata.blob.core.windows.net/camhd?' + sas_token

    for i, url in enumerate(transfer_list):
        filename = url.split('/')[-1].strip()
        size = int(requests.get(url, stream=True).headers['Content-length'])/1024/1024/1024

        if max_file_size is not None:
            if size > max_file_size:
                print('%s  Skipping %s (%.1f GB)' % (datetime.datetime.now(), filename, size))
                continue

        print('%s  Transferring %s (%.1f GB)' % (datetime.datetime.now(), filename, size))
        subprocess.check_output(['wget', '-q', '-O', '/mnt/opendata/%s' % filename, url])
        subprocess.check_output(['/usr/local/bin/azcopy', 'copy',
                                 '/mnt/opendata/%s' % filename, container, '--put-md5'])
        subprocess.check_output(['rm', '/mnt/opendata/%s' % filename])


# delete files on Azure for testing
#for file_url in files_to_transfer:
#    filename = file_url.split('/')[-1].strip()
#    print('Deleting %s' % filename)
#    blob_client = blob_service_client.get_blob_client(container='camhd', blob=filename)
#    blob_client.delete_blob()


def read_dbcamhd():
    """
    Open the current dbcamhd.json file.
    """
    dbcamhd_url = 'https://ooiopendata.blob.core.windows.net/camhd/dbcamhd.json'
    with fsspec.open(dbcamhd_url) as f:
        dbcamhd = pd.read_json(f, orient="records", lines=True, convert_dates=False, dtype=False)
    return dbcamhd


def get_deployment(timestamp):
    """
    Return the deployment number of a CAMHD file based on epoch timestamp.

    Parameters
    ----------
    timestamp : int
        Unix epoch timestamp from camhd.get_timestamp().

    Returns
    -------
    int
        The deployment number.
    """
    dt = datetime.datetime.fromtimestamp(timestamp)
    if dt < datetime.datetime(2016,7,26,21,18,0):
        return 2
    elif dt >= datetime.datetime(2016,7,26,21,18,0) and dt < datetime.datetime(2017,8,14,6,0,0):
        return 3
    elif dt >= datetime.datetime(2017,8,14,6,0,0) and dt < datetime.datetime(2018,7,4,0,0,0):
        return 4
    elif dt >= datetime.datetime(2018,7,4,0,0,0) and dt < datetime.datetime(2019,6,16,2,0,0):
        return 5
    elif dt >= datetime.datetime(2019,6,16,2,0,0) and dt < datetime.datetime(2020,8,6,11,0,0):
        return 6
    elif dt >= datetime.datetime(2020,8,6,11,0,0):
        return 7
    else:
        return None

def get_dbcamhd_entry(blob):
    """
    Return a dbcamhd database entry for CAMHD file blob.

    Parameters
    ----------
    blob : Azure BlobProperties object.
        Blob corresponding to a CAMHD file in the ooiopendata storage account.

    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame with one data row.
    """
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
                        columns=['name', 'url', 'filesize', 'md5', 'moov', 'timestamp',
                                   'deployment', 'frame_count'])


def update_dbcamhd(dbcamhd):
    """
    Update the dbcamhd database.

    Parameters
    ----------
    dbcamhd: pandas.DataFrame
        Pandas DataFrame containing the dbcamhd database.
    """
    # get list of blobs
    ooiopendata_blobs = get_ooiopendata_blobs(container='camhd')

    # filter blobs already in database
    blob_list = []
    for blob in ooiopendata_blobs:
        if blob.name not in dbcamhd['name'].values and blob.name.endswith('mov'):
            blob_list.append(blob)

    # update dbcamhd
    for i, blob in enumerate(blob_list):
        if i == 0:
            dbcamhd_new = get_dbcamhd_entry(blob)
        else:
            dbcamhd_new = pd.concat([dbcamhd_new, get_dbcamhd_entry(blob)])

    if len(blob_list) > 0:
        dbcamhd = pd.concat([dbcamhd, dbcamhd_new]).reset_index(drop=True)
    return dbcamhd

def save_dbcamhd(dbcamhd, sas_token=None):
    """
    Save the dbcamhd database.

    Parameters
    ----------
    dbcamhd: pandas.DataFrame
        Pandas DataFrame containing the dbcamhd database.
    sas_token : str
        SAS token required for authenticated access.
    """
    storage_account_url = 'https://ooiopendata.blob.core.windows.net'
    blob_service_client = BlobServiceClient(storage_account_url, credential=sas_token)

    dbcamhd.to_json('dbcamhd.json', orient="records", lines=True)
    #blob_client = blob_service_client.get_blob_client(container='camhd', blob='dbcamhd.json')
    #with open('dbcamhd.json', 'rb') as data:
    #    blob_client.upload_blob(data, overwrite=True)

    dbcamhd.to_csv('dbcamhd.csv')
    #blob_client = blob_service_client.get_blob_client(container='camhd', blob='dbcamhd.csv')
    #with open('dbcamhd.csv', 'rb') as data:
    #    blob_client.upload_blob(data, overwrite=True)


def main():
    # load SAS token from secrets file
    secrets_file = '/home/tjc/github/ooicloud/ooi-opendata/secrets/tjcrone.yml'
    with open(secrets_file, 'r') as stream:
        keys = yaml.safe_load(stream)
    sas_token = keys['camhd']

    # get list of files to transfer
    raw_list = get_raw_list(days=10)
    ooiopendata_list = get_ooiopendata_list(container='camhd')
    transfer_list = get_transfer_list(raw_list, ooiopendata_list)

    # transfer files
    transfer_files(transfer_list, sas_token, max_file_size=40)

    # open database file
    dbcamhd = read_dbcamhd()

    # update dbcamhd
    dbcamhd = update_dbcamhd(dbcamhd)

    # save dbcamhd
    save_dbcamhd(dbcamhd, sas_token=sas_token)


if __name__ == '__main__':
    main()
