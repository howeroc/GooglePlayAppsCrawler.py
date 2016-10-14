import argparse
import requests
import sys
import errno
from lxml import html
from shared.Utils import Utils
from shared.Utils import HTTPUtils
from shared.Parser import parser as html_parser
from shared.TorProxy import TorProxy

class Worker:

    def __init__(self):
        """
        Class Constructor : Initializes MongoDB
        configuration on a dictionary
        """

        params = {}
        # params['server'] = 'mobiledata.bigdatacorp.com.br'
        # params['port'] = '21766'
        # params['server'] = '45.32.49.146'
        params['server'] = '127.0.0.1'
        params['port'] = '27017'
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
                            help='Max http errors allowed on workers \
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
        self._mongo_wrapper.ensure_index('_id', self._params['apps_collection'])

        # Proxies Loading
        self._proxies = Utils.load_proxies(self._args)

        # if "Debug Http" is set to true, "verify" must be "false"
        self._verify_certificate = not self._args['debug_https']
        self._is_using_proxies = self._proxies != None

        # Control Variables - Used on the 'retrying logic'
        retries, max_retries = 0, 8

        parser = html_parser()

        # Loop only breaks when there are no more apps to be processed
        while True:

            # Finds an app to be processed and toggles it's state to 'Busy'
            seed_record = self._mongo_wrapper.find_and_modify()

            if not seed_record:
                break

            try:
                url = seed_record['_id']

                # Do we need to normalize the url ?
                if 'http://' not in url and 'https://' not in url:
                    url = 'https://play.google.com' + url

                self._logger.info('Processing: %s' % url)

                # Is this app processed already ?
                if self._mongo_wrapper.app_processed(url, self._params['apps_collection']):

                    self._logger.info('Duplicated App : %s. Skipped' % url)
                    self._mongo_wrapper.remove_app_from_queue(seed_record)
                    continue

                # Get Request for the App's Page
                response = requests.get(url + "&hl=en&gl=us",
                                        HTTPUtils.headers,
                                        verify=self._verify_certificate,
                                        # proxies=Utils.get_proxy(self))
                                        proxies=TorProxy.get_proxy())

                # Sanity Checks on Response
                if not response.text or response.status_code != requests.codes.ok:
                    self._logger.info('Error Opening App Page : %s' % url)

                    retries += 1

                    # stop try if retry too many times
                    if retries > max_retries:
                        continue

                    # Retries logic are different if proxies are being used
                    if self._is_using_proxies:
                        Utils.sleep()
                try:
                    # Scraping Data from HTML
                    app = parser.parse_app_data(response.text)

                    # Stamping URL into app model
                    app['Url'] = url
                    app['_id'] = url

                    url_arr = url.split("?id=")
                    if isinstance(url_arr, list) and len(url_arr) > 1 and not url_arr[1] is None:
                        app['PkgName'] = url_arr[1]
                    else:
                        app['PkgName'] = ''

                    # Reaching related apps
                    related_apps = parser.parse_related_apps(response.text)

                    if not related_apps:
                        app['RelatedUrls'] = None
                    else:
                        app['RelatedUrls'] = related_apps
                        self._logger.info('Related Apps: %s - %d' % (url, len(related_apps)))

                    # Inserting data into MongoDB
                    self._mongo_wrapper._insert(app, self._params['apps_collection'])

                    # Re-Feeding seed collection with related-app urls
                    # if app['RelatedUrls']:
                    #     for url in app['RelatedUrls']:
                    #         if not self._mongo_wrapper.app_processed(url, self._params['apps_collection']) and \
                    #            not self._mongo_wrapper.app_processed(url, self._params['seed_collection']):
                    #             self._mongo_wrapper.insert_on_queue(url, self._params['seed_collection'])

                except Exception as exception:
                    self._logger.error(exception)

                    # Toggling app state back to false
                    self._mongo_wrapper.toggle_app_busy(url,False, self._params['seed_collection'])

            except Exception as exception:
                self._logger.error(exception)



if __name__ == '__main__':
    requests.packages.urllib3.disable_warnings()
    worker = Worker()
    worker.scrape_apps()
