import argparse
import requests
import sys
import errno
from lxml import html
from shared.Utils import Utils

class Worker:

    def __init__(self):
        """
        Class Constructor : Initializes MongoDB
        configuration on a dictionary
        """

        params = {}
        params['server'] = 'mobiledata.bigdatacorp.com.br'
        params['port'] = '21766'
        params['database'] = 'MobileAppsData'
        params['username'] = 'GitHubCrawlerUser'
        params['password'] = 'g22LrJvULU5B'
        params['seed_collection'] = 'PlayStore_QueuedApps_2015_05_PY'
        params['apps_collection'] = 'PlayStore_2015_08_PY'
        params['auth_database'] = 'MobileAppsData'
        params['write_concern'] = True
        self._params = params

    def get_arguments_parser(self):
        """
        Creates a parsing object using the argsparse
        lib
        """

        parser = argparse.ArgumentParser(description='Scraper / Worker layer \
                                                     of the Google Play Store \
                                                     Crawler')

        # All arguments start with "-", hence, they are all handled as optional
        parser.add_argument('--console-log-verbosity',
                            type=str,
                            choices=['INFO', 'DEBUG', 'WARN',
                                     'ERROR', 'CRITICAL'],
                            help='Log Verbosity Level (default=INFO)',
                            default='INFO')

        parser.add_argument('--file-log-verbosity',
                            type=str,
                            choices=['INFO', 'DEBUG', 'WARN',
                                     'ERROR', 'CRITICAL'],
                            help='Log Verbosity Level (default=ERROR)',
                            default='ERROR')

        parser.add_argument('--log-file',
                            type=str,
                            help='Path of the output log file (default=\
                                  console-only logging)')

        parser.add_argument('--max-errors',
                            type=int,
                            default=100,
                            help='Max http errors allowed on Bootstrapping \
                                 phase. (default=100)')

        parser.add_argument('--debug-https',
                            action='store_true',
                            default=False,
                            help='Turn this flag on to enable Fiddler to \
                                 hook and debug HTTPS Requests')

        parser.add_argument('--proxies-path',
                            type=file,
                            default=None,
                            help='Path to the file of proxies \
                            (read the documentation)')

        return parser

    def scrape_apps(self):
        """
        Main method of the 'Worker' layer of this project.

        This method starts the distributed working phase which will
        consume urls from the seed database and scrape apps data out
        of the html pages, storing the result into the
        apps_data collection on MongoDB
        """

        # Arguments Parsing
        args_parser = self.get_arguments_parser()
        self._args = vars(args_parser.parse_args())

        # Log Handler Configuring
        self._logger = Utils.configure_log(self._args)

        # MongoDB Configuring
        if not Utils.configure_mongodb(self,**self._params):
            self._logger.fatal('Error configuring MongoDB')
            sys.exit(errno.ECONNREFUSED)

        # Making sure indexes exist
        self._mongo_wrapper.ensure_index('IsBusy');

        # Loop only breaks when there are no more apps to be processed
        while True:

            # Finds an app to be processed and toggles it's state to 'Busy'
            seed_record = self._mongo_wrapper.find_and_modify()

            if not seed_record:
                break

            

if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    worker = Worker()
    worker.scrape_apps()
