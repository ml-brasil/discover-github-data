import codecs
import octograb

__all__ = ['export_inputs']

logger = octograb.utils.get_logger('PRESELECTION:EXPORT')

def export_inputs():
    logger.info('Initializing EXPORT_INPUTS.')
    _c = octograb.config

    archives_name = _c['cache_dir']+'/archives.csv'
    input_name_t = _c['input_dir']+'/input.%04d.csv'

    # read all repositories
    logger.info('Reading "archives.csv"...')
    f = codecs.open(archives_name, 'r', 'utf-8')
    repos = f.readlines()
    f.close()

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
        lines = '\n'.join(items)

        logger.debug('Processing file %d of %d.'%(i, _files))
        logger.info('Saving input "%s"...'%name)
        with codecs.open(name, 'w', 'utf-8') as f:
            f.write('stars,forks,commits,name\n')
            f.write(lines)
        logger.info('... input saved.')

        n += step
        i += 1

        logger.debug('%d repositories processed.'%n)

    logger.info('EXPORT_INPUTS finished.')

