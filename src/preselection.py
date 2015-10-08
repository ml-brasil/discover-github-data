import sys
import os
import codecs
import cPickle
import datetime
import gzip
import logging
import json
import operator

import utils

# CONSTANTS ===================================================================
config = dict(
    archive_base   = 'http://data.githubarchive.org',
    inputs_dir     = 'inputs',
    items_per_file = 2400,
    log_file       = 'preselection.log',
    
    # initial and final date to search the archive
    from_year      = 2015,
    from_month     = 1,
    from_day       = 1,
    from_hour      = 0,
    # to_year      = datetime.datetime.now().year,
    # to_month     = datetime.datetime.now().month,
    # to_day       = datetime.datetime.now().day,
    # to_hour      = datetime.datetime.now().hour,
    to_year        = 2015,
    to_month       = 1,
    to_day         = 1,
    to_hour        = 10,

)
# =============================================================================

# SET UP ======================================================================
logging.basicConfig(
    filename=config['log_file'],
    format='[%(asctime)s:%(levelname)s:%(name)s] %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger('PRESELECTION')
# =============================================================================

# =============================================================================
class Preselection(object):
    '''This class is responsible to download and process github archive files, 
    and to generate the input files used in the crawler.'''

    def __init__(self):
        '''Constructor.'''

        # date follows the structure of the github archive name convention
        self.current_date = datetime.datetime(
            config['from_year'],
            config['from_month'],
            config['from_day'],
            config['from_hour'],
        )
        # a dict where the key is the name of the repository and the items are
        #    the number of stars, forks, commits and pull requests of the repo
        self.current_set = {}

    def __save_state(self):
        '''Saves the execution state in pickle format.'''

        logger.info('Saving state...')

        state = {
            'date' : self.current_date,
            'set'  : self.current_set
        }
        data = cPickle.dumps(state)
        utils.safe_save(data, 'preselection.state')

        logger.info('State saved.')

    def __load_state(self):
        '''Loads the execution state.'''

        try:
            logger.info('Trying to load state...')

            data = open('preselection.state')
            state = cPickle.load(data)
            self.current_date = state['date']
            self.current_set = state['set']
            data.close()

            logger.info('... state loaded successfully.')

        except IOError:
            logger.info('... no state found.')

    def __update_repository(self, name, commits=0, stars=0, forks=0, pulls=0):
        '''Updates the stats of a single repository.'''

        # if repository isn't registered yet
        if name not in self.current_set:
            self.current_set[name] = {
                'stars'        : stars,
                'commits'      : commits,
                'forks'        : forks,
                'pullrequests' : pulls
            }
            return

        # if repository is already registered
        if commits: self.current_set[name]['commits'] += commits
        if stars: self.current_set[name]['stars'] += stars
        if forks: self.current_set[name]['forks'] += forks
        if pulls: self.current_set[name]['pullrequests'] += pulls

    def __process_event(self, event):
        '''Process a single line of the archive file.'''

        data = json.loads(event)
        type_ = data['type']
        name = data['repo']['name']

        if type_ == 'WatchEvent':
            self.__update_repository(name, stars=1)

        elif type_ == 'PushEvent':
            commits = len(data['payload']['commits'])
            self.__update_repository(name, commits=commits)

        elif type_ == 'ForkEvent':
            self.__update_repository(name, forks=1)

        elif type_ == 'PullRequestEvent':
            self.__update_repository(name, pulls=1)

    def __process_file(self):
        '''Downloads and process an archive file.'''

        # Download and unzip the archive
        base_name = utils.get_archive_name(self.current_date)
        logger.info('Processing archive %s...'%base_name)

        url = '%s/%s.json.gz'%(config['archive_base'], base_name)
        zip_name = '%s.json.gz'%base_name

        logger.info('Downloading archive "%s"...'%url)
        utils.download_file(url, zip_name)
        logger.info('... download complete.')

        # Read and process archive
        logger.info('Processing events...')
        f = gzip.open(zip_name)
        for line in f:
            self.__process_event(line)
        f.close()
        logger.info('... events processed.')

        # Remove downloaded file
        logger.info('Removing archive.')
        os.remove(zip_name)

        logger.info('... archive %s processed.'%base_name)

    def __export_repositories(self):
        '''Export the current state to the input files.'''

        n = len(self.current_set)
        logger.info('Exporting %d repositories...'%n)

        # convert the dict into a list, so we can sort it
        d = self.current_set
        repos = [(k, v['stars'], v['forks'], v['commits']) for k, v in d.iteritems()]

        # sorting by stars, forks and commits, in this order
        logger.info('Sorting repositories.')
        repos.sort(key=operator.itemgetter(1, 2, 3), reverse=True)
        
        # split and save files
        logger.info('Splitting and saving files.')
        size = len(repos)
        step = config['items_per_file']
        dir_ = config['inputs_dir']
        i = 0
        j = 0
        while i < size:
            items = repos[i: i+step]

            with codecs.open(dir_+'/repositories.%04d'%j, 'w', 'utf-8') as f:
                f.write('stars,forks,commits,name\n')
                for item in items:
                    f.write('%05d,%05d,%07d,"%s"\n'%(item[1], 
                                                     item[2],
                                                     item[3],
                                                     item[0]))

            j += 1
            i += step


    def run(self):
        '''Run the proselection. You can safely stop the execution of it.'''

        logger.info('Initiating preselection.')

        # load previous state
        self.__load_state()

        # create input directory
        dir_ = config['inputs_dir']
        if not os.path.isdir(dir_):
            os.mkdir(dir_)

        # progress variables
        date_step = datetime.timedelta(hours=1)
        max_date = datetime.datetime(
            config['to_year'],
            config['to_month'],
            config['to_day'],
            config['to_hour'],
        )

        # grab archives
        logger.info('Grabbing archives...')
        while self.current_date < max_date:
            # process a single archive
            self.__process_file()

            # save state
            self.current_date += date_step
            self.__save_state()

            # debug
            n = len(self.current_set)
            logger.debug('Total repositories registered so far: %d'%n)

        # debug
        n = sys.getsizeof(self.current_set)/1024./1024.
        logger.debug('Total size of repositories: %.4fMb'%n)
        logger.info('... all archives processed.')

        # export repositories
        self.__export_repositories()
# =============================================================================


if __name__ == '__main__':
    app = Preselection()
    app.run()