import codecs
import pandas
import octograb

__all__ = ['export_inputs']

logger = octograb.utils.get_logger('PRESELECTION:EXPORT')

def export_inputs():
    logger.info('Initializing EXPORT_INPUTS.')
    _c = octograb.config

    archives_name = _c['cache_dir']+'/archives.csv'
    input_name_t = _c['input_dir']+'/input.%04d.csv'
    min_stars = _c['preselection']['min_stars']
    min_forks = _c['preselection']['min_forks']
    min_commits = _c['preselection']['min_commits']

    # read all repositories
    logger.debug('Loading dataset with pandas.')
    repos = pandas.read_csv(archives_name)
    logger.debug('%d repositories loaded.'%len(repos))

    # selecting data
    logger.debug('Selecting dataset.')
    repos = repos[(repos['stars']>=min_stars) & \
                  (repos['forks']>=min_forks) & \
                  (repos['commits']>=min_commits)]
    logger.debug('%d resulting repositories.'%len(repos))


    # split repositories in batches
    step = _c['input_per_file']
    N = len(repos)
    n = 0
    i = 0

    _files = N/step + 1
    logger.info('Splitting %d repositories into %d archives.'%(N, _files))
    while n < N:
        name = input_name_t%i
        items = repos[n: n+step]
        # lines = '\n'.join(items)

        logger.debug('Processing file %d of %d.'%(i, _files))
        logger.info('Saving input "%s"...'%name)
        with codecs.open(name, 'w', 'utf-8') as f:
            f.write('stars,forks,commits,name\n')
            for _, item in items.iterrows():
                f.write('%05d,%05d,%07d,"%s"\n'%(item[0], 
                                                 item[1],
                                                 item[2],
                                                 item[3]))
        logger.info('... input saved.')

        n += step
        i += 1

        logger.debug('%d repositories processed.'%n)

    logger.info('EXPORT_INPUTS finished.')

