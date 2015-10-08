import os
import re
import json
import requests
import logging
import codecs

__all__ = ['get_archive_name', 'get_logger', 'safe_save', 'download_file', 
           'load_json', 'make_dir', 'archive_to_csv']

def get_archive_name(date):
    '''Returns the archive name given a datetime object.'''
    return date.strftime('%Y-%m-%d-') + str(int(date.strftime('%H')))

def archive_to_csv(data):
    lines = ['%05d,%05d,%07d,"%s"'%(_[1], _[2], _[3], _[0]) for _ in data]
    return '\n'.join(lines)

def safe_save(data, path, no_bkp=False):
    # save file
    f = open(path+'-bkp', 'w')
    f.write(data)
    f.close()

    # erase previous if any
    if os.path.isfile(path): os.remove(path)
    os.rename(path+'-bkp', path)
    if os.path.isfile(path+'-bkp') and no_bkp: os.remove(path+'-bkp')

def download_file(url, path):
    r = requests.get(url, stream=True)

    f = open(path, 'wb')
    for chunk in r.iter_content(chunk_size=1024): 
        if chunk:
            f.write(chunk)
    f.close()

def get_logger(name):
    return logging.getLogger(name)

def load_json(path):
    content = codecs.open(path, 'r', 'utf-8').read()
    content = re.sub(re.compile("/\*.*?\*/", re.DOTALL) , "", content)
    content = re.sub(re.compile("[\s|\n]//.*?\n" ) , "", content)
    return json.loads(content)

def make_dir(path):
    if not os.path.isdir(path):
        os.mkdir(path)
