import logging
from octograb import utils
from octograb import models

__all__ = ['config', 'configure']

# Globals
config = {}


def configure(config_name):
    global config
    config = utils.load_json(config_name)

    logging.basicConfig(
        filename = config['log_file'],
        format   = '[%(asctime)s:%(levelname)s:%(name)s] %(message)s',
        level    = logging.DEBUG
    )

    utils.make_dir(config['input_dir'])
    utils.make_dir(config['cache_dir'])
    utils.make_dir(config['cache_dir']+'/'+config['preselection']['archives_dir'])