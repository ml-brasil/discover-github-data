import csv
import glob
import codecs
import octograb

__all__ = ['merge_archives']

logger = octograb.utils.get_logger('PRESELECTION:MERGE')


# MAIN ========================================================================
def merge_archives():
    logger.info('Initializing MERGE_ARCHIVES.')

    dataset = octograb.models.ArchiveDataset()
    
    _c = octograb.config
    pattern = _c['cache_dir']+'/'+_c['preselection']['archives_dir']+'/*.csv'
    for path in glob.iglob(pattern):
        logger.info('Processing "%s".'%path)
        with codecs.open(path, 'r', 'utf-8') as f:
            for i, line in enumerate(f):
                try:
                    items = line.split(',')
                    stars = int(items[0])
                    forks = int(items[1])
                    commits = int(items[2])
                    name = items[3].strip('\n\r"')
                except Exception as e:
                    logger.error('Error in line %d of file %s'%(i, path))
                    logger.error('   erro message: "%s"'%e.message)

                dataset.update(name, stars=stars, commits=commits, forks=forks)
        logger.info('... "%s" processed.'%path)
    
    logger.info('Saving final dataset "archives.csv"...')
    name = _c['cache_dir']+'/archives.csv'

    logger.debug('Ordering repositories.')
    data = dataset.export()

    logger.debug('Converting data to csv.')
    header = 'stars,forks,commits,name'
    body = octograb.utils.archive_to_csv(data)
    s = '\n'.join(header, body)
    
    logger.debug('Saving.')
    octograb.utils.safe_save(s, name, no_bkp=True)
    logger.info('... dataset saved.')

    logger.debug('Total repositories: %d.'%len(data))
    logger.info('MERGE_ARCHIVES finished.')
# =============================================================================
