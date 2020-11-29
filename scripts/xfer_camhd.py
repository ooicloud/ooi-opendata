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
import fcntl


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
    list : tuple
        Returns a list of tuples with the files to transfer and their sizes.
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
        else:
            transfer_list.append(url)

    size_list = []
    for url in transfer_list:
        size_list.append(requests.get(url, stream=True).headers['Content-length'])

    return [(transfer_list[i], size_list[i]) for i in range(len(transfer_list))]


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

    for item in transfer_list:
        url = item[0]
        filename = url.split('/')[-1].strip()
        file_size = int(item[1])/(1024**3)

        if max_file_size is not None:
            if file_size > max_file_size:
                continue
        logmessage('Copy: %s (%.2f GB)' % (filename, file_size))
        subprocess.check_output(['wget', '-q', '-O', ('/mnt/opendata/%s' % filename), url])
        subprocess.check_output(['/usr/local/bin/azcopy', 'copy',
                                 ('/mnt/opendata/%s' % filename), container, '--put-md5'])
        subprocess.check_output(['rm', ('/mnt/opendata/%s' % filename)])


def log_transfer_stats(transfer_list, max_file_size=None):
    """
    Print file transfer stats.

    Parameters
    ----------
    tranfer_list : list of str
        List of files to transfer.
    max_file_size : int, optional
        Maximum file size to transfer.
    """
    skip_list = []
    noskip_list = []
    for item in transfer_list:
        url = item[0]
        filename = url.split('/')[-1].strip()
        file_size = int(item[1])/(1024**3)

        if max_file_size is not None:
            if file_size > max_file_size:
                skip_list.append(item)
            else:
                noskip_list.append(item)
        else:
            noskip_list.append(item)

    total_size = sum([int(item[1]) for item in noskip_list])/(1024**3)
    logmessage('Number of files to skip: %i' % len(skip_list))
    for item in skip_list:
        url = item[0]
        filename = url.split('/')[-1].strip()
        logmessage('Skip: %s (%.2f GB)' % (filename, int(item[1])/(1024**3)))
    logmessage('Number of files to transfer: %i (%.2f GB)' % (len(noskip_list), total_size))


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
    file_size = blob.size
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

    return pd.DataFrame([[name, url, file_size, md5, moov, timestamp, deployment, frame_count]],
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
    blob_client = blob_service_client.get_blob_client(container='camhd', blob='dbcamhd.json')
    with open('dbcamhd.json', 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)

    dbcamhd.to_csv('dbcamhd.csv')
    blob_client = blob_service_client.get_blob_client(container='camhd', blob='dbcamhd.csv')
    with open('dbcamhd.csv', 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)


def logmessage(message):
    name = __file__.split('/')[-1].split('.')[0]
    print('%s [%s] %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), name, message))


def main():
    # check for lock
    lock_file = '/home/tjc/github/ooicloud/ooi-opendata/scripts/xfer_camhd.lock'
    lock = open(lock_file, 'w')
    try:
        fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except:
        raise SystemExit
    logmessage('Lock obtained')

    # begin transfer
    logmessage('Starting transfer of CAMHD files')

    # load SAS token from secrets file
    secrets_file = '/home/tjc/github/ooicloud/ooi-opendata/secrets/tjcrone.yml'
    with open(secrets_file, 'r') as stream:
        keys = yaml.safe_load(stream)
    sas_token = keys['camhd']

    # get list of files to transfer
    raw_list = get_raw_list(days=3)
    ooiopendata_list = get_ooiopendata_list(container='camhd')
    transfer_list = get_transfer_list(raw_list, ooiopendata_list)

    # transfer files
    max_file_size = 40
    log_transfer_stats(transfer_list, max_file_size=max_file_size)
    transfer_files(transfer_list, sas_token, max_file_size=max_file_size)
    logmessage('Transfer complete')

    # open database file
    logmessage('Updating database')
    dbcamhd = read_dbcamhd()

    # update dbcamhd
    dbcamhd = update_dbcamhd(dbcamhd)

    # save dbcamhd to local and remote
    save_dbcamhd(dbcamhd, sas_token=sas_token)
    logmessage('Database update complete')

    # remove lock
    lock.close()
    subprocess.check_output(['rm', lock_file])

if __name__ == '__main__':
    main()
