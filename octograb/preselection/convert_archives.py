import socket
import time
import os
import gzip
import json
import datetime
import cPickle

import requests

import octograb

__all__ = ['convert_archives']

logger = octograb.utils.get_logger('PRESELECTION:CONVERT')

# MAIN ========================================================================
def convert_archives():
    logger.info('Initializing CONVERT_ARCHIVES.')

    # progress variables
    cur_date = _load_state()
    max_date = _max_date()
    step = datetime.timedelta(days=1)

    # main loop
    while cur_date < max_date:
        _process_day(cur_date)
        cur_date += step

        _save_state(cur_date)

    logger.info('CONVERT_ARCHIVES finished.')
# =============================================================================

# HELPERS =====================================================================
def _load_state():
    state_name = octograb.config['cache_dir'] + '/' + 'preselection.cache'
    date = None
    
    logger.info('Trying do load state...')
    if os.path.isfile(state_name):
        f = open(state_name, 'r')
        date = cPickle.load(f)
        f.close()
        logger.info('... state loaded successfully.')

    else:
        date = datetime.datetime(
            octograb.config['preselection']['from_year'],
            octograb.config['preselection']['from_month'],
            octograb.config['preselection']['from_day'],
            0
        )
        logger.info('... no state found.')

    return date

def _save_state(date):
    state_name = octograb.config['cache_dir'] + '/' + 'preselection.cache'

    logger.info('Saving state...')
    data = cPickle.dumps(date)
    octograb.utils.safe_save(data, state_name)
    logger.info('... state saved.')

def _max_date():
    return datetime.datetime(
        octograb.config['preselection']['to_year'],
        octograb.config['preselection']['to_month'],
        octograb.config['preselection']['to_day'],
        0
    )

def _process_day(date):
    _name = date.strftime('%Y-%m-%d')
    logger.info('Converting data for archive %s...'%_name)

    dataset = octograb.models.ArchiveDataset()
    step = datetime.timedelta(hours=1)

    # get all archive names
    _f = octograb.utils.get_archive_name
    archive_names = [_f(date+step*i)+'.json.gz' for i in xrange(24)]

    # get all zip names
    _c = octograb.config
    _path = _c['cache_dir'] + '/' + _c['preselection']['archives_dir'] + '/'
    archive_paths = [_path+n for n in archive_names]

    # get all urls
    _base = _c['preselection']['archives_url'] + '/'
    archive_urls  = [_base+n for n in archive_names]
    
    # download them all
    for url, path, name in zip(archive_urls, archive_paths, archive_names):
        logger.info('Downloading "%s"...'%name)
        
        # repeat until download is completed
        _download_complete = False
        while not _download_complete:
            try:
                octograb.utils.download_file(url, path)
                _download_complete = True

            # handle connection error
            except (socket.error, requests.exceptions.ConnectionError) as e:
                logger.error('Connection error, trying again after 15 seconds.')
                time.sleep(15)

        logger.info('... download completed.')

    # open them all
    for path, name in zip(archive_paths, archive_names):
        logger.info('Processing "%s"...'%name)
        _process_file(path, dataset)
        logger.info('... "%s" processed'%name)

    # export dataset
    logger.info('Exporting %s...'%_name)
    data = dataset.export()
    s = octograb.utils.archive_to_csv(data)
    octograb.utils.safe_save(s, _path+_name+'.csv', no_bkp=True)
    logger.info('... exporting completed.')

    # remove downloaded files
    for path in archive_paths:
        try:
            os.remove(path)

        # ignore if cant remove file
        except WindowsError as e:
            logger.error('Could not remove "%s": "%s"'%(path, e.message))

    logger.info('... archive %s converted.'%_name)

def _process_file(path, dataset):
    f = gzip.open(path)
    for line in f:
        _process_event(line, dataset)
    f.close()

def _process_event(line, dataset):
    # don't stop processing
    try:
        data  = json.loads(line)
    except ValueError as e:
        logger.error(e.message)
        return

    type_ = data['type']
    name  = data['repo']['name']

    if type_ == 'WatchEvent':
        dataset.update(name, stars=1)

    elif type_ == 'PushEvent':
        commits = len(data['payload']['commits'])
        dataset.update(name, commits=commits)

    elif type_ == 'ForkEvent':
        dataset.update(name, forks=1)

    elif type_ == 'PullRequestEvent':
        dataset.update(name, pulls=1)
# =============================================================================
