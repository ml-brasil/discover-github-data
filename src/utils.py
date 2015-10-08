import os
import requests

def get_archive_name(date):
    return date.strftime('%Y-%m-%d-') + str(int(date.strftime('%H')))

def safe_save(data, path):
    # save file
    f = open(path+'-temp', 'w')
    f.write(data)
    f.close()

    # erase previous if any
    if os.path.isfile(path): os.remove(path)
    os.rename(path+'-temp', path)
    if os.path.isfile(path+'-temp'): os.remove(path+'-temp')

def download_file(url, filename):
    r = requests.get(url, stream=True)

    f = open(filename, 'wb')
    for chunk in r.iter_content(chunk_size=1024): 
        if chunk:
            f.write(chunk)
    f.close()