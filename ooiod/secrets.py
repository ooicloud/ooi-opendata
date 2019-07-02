import os
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import ContainerPermissions
import datetime
import pickle

def load_keys(secrets_file):
    secrets_file = os.path.abspath(secrets_file)
    if os.path.isfile(secrets_file):
        with open(secrets_file, 'rb') as f:
            keys = pickle.load(f)
        print('You have:')
        for key in keys:
            print('    ' + key)
        return keys
    else:
        raise NameError('Secrets file not found.')

def gen_token(container, account_key, expiration):
    ooiopendata_service = BlockBlobService('ooiopendata', account_key)
    container_sas = ooiopendata_service.generate_container_shared_access_signature(container,
        ContainerPermissions.READ +
        ContainerPermissions.WRITE +
        ContainerPermissions.DELETE +
        ContainerPermissions.LIST,
        datetime.datetime.utcnow() + expiration)
    return container_sas

def write_pickle(containers, account_key, expiration, filename, overwrite=False, include_account_key=False):
    if type(containers) == list:
        keys = dict()
        for container in containers:
            container_sas = gen_token(container, account_key, expiration)
            keys[container] = container_sas
        if include_account_key == True:
            keys['ooiopendata'] = account_key
        pickle_path = os.path.abspath(filename)
        print(pickle_path)
        if os.path.isfile(pickle_path) == False:
            with open(pickle_path, 'wb') as f:
                pickle.dump(keys, f, protocol=0)
        elif overwrite == True:
            with open(pickle_path, 'wb') as f:
                pickle.dump(keys, f, protocol=0)
        else:
            raise Exception('File exists.')
    else:
        raise TypeError('The \'containers\' argument must be a list.')
