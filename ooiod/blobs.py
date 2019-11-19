import requests
from xml.etree import ElementTree

def _list_blob_set(storage_account, container, nextmarker = None):
    request_uri = 'https://%s.blob.core.windows.net/%s?restype=container&comp=list' % (storage_account, container)
    if nextmarker:
        request_uri = request_uri + '&marker=' + nextmarker
    catalog_xml = requests.get(request_uri).content
    catalog_tree = ElementTree.fromstring(catalog_xml)
    blob_set = []
    for blob in catalog_tree.find('Blobs').findall('Blob'):
        blob_set.append(blob.find('Url').text)
    nextmarker = catalog_tree.find('NextMarker').text
    return blob_set, nextmarker

def list_blobs(storage_account, container):
    blobs = []
    blob_set, nextmarker = _list_blob_set('ooiopendata', 'camhd')
    blobs = blobs + blob_set
    while nextmarker:
        blob_set, nextmarker = _list_blob_set('ooiopendata', 'camhd', nextmarker)
        blobs = blobs + blob_set
    return blobs
