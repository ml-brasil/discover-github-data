import operator

__all__ = ['ArchiveDataset']

class ArchiveDataset(dict):
    def __init__(self):
        self.__set = {}

    def update(self, name, stars=0, commits=0, forks=0, pulls=0):
        # if repository isn't registered yet
        if name not in self.__set:
            self.__set[name] = {
                'stars'   : stars,
                'commits' : commits,
                'forks'   : forks,
                'pulls'   : pulls
            }
            return

        # if repository is already registered
        if stars   : self.__set[name]['stars'] += stars
        if commits : self.__set[name]['commits'] += commits
        if forks   : self.__set[name]['forks'] += forks
        if pulls   : self.__set[name]['pulls'] += pulls

    def export(self):
        # convert the dict into a list, so we can sort it
        d = self.__set
        repos = [(k, v['stars'], v['forks'], v['commits']) for k, v in d.iteritems()]

        # sorting by stars, forks and commits, in this order
        repos.sort(key=operator.itemgetter(1, 2, 3), reverse=True)
        
        return repos