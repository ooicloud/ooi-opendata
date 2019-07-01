import glob, os

def get_keys(secrets_path):
    secrets_path = os.path.abspath(secrets_path)
    if os.path.isdir(secrets_path):
        keys = dict()
        for filename in glob.iglob(secrets_path + '/**', recursive=True):
            if os.path.isfile(filename):
                with open(filename, 'rb') as b:
                    b.seek(1)
                    if b.read(8) != b'GITCRYPT':
                        b.seek(0,0)
                        keys[filename.split('/')[-1]] = b.readline().strip().decode()
        if len(keys) > 0:
            return keys
        else:
            raise Exception('No keys found.')
    else:
        raise NameError('Secrets directory not found.')
