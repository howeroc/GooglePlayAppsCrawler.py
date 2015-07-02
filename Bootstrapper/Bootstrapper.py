import logging
import argparse
import BootstrappingSeed
import requests
import sys
import errno
from lxml import html
from Shared.MongoWrapper import MongoDBWrapper


class Bootstrapper:

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
        params['auth_database'] = 'MobileAppsData'
        params['write_concern'] = True
        self._params = params

        # Urls Hashset
        self._parsed_urls = set()

    def get_arguments_parser(self):
        """
        Creates a parsing object using the argsparse
        lib
        """

        parser = argparse.ArgumentParser(description='Bootstrapping phase of \
                                                     the Google Play \
                                                     Store Crawler')

        parser.add_argument('bootstrapping-terms',
                            type=file,
                            help='Path to the xml containing the bootstrapping \
                                  terms that should be loaded')

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
                            help='Path of the output log file (default is \
                                  console-only logging)')

        return parser

    def get_log_level_from_string(self, logLevel):
        """
        Returns the proper logging level based
        on the string received.

        Positional Arguments:
            - logLevel (str) - Log level as a string

        returns the logging level (int) that matches the string received
        """

        if logLevel == 'DEBUG':
            return logging.DEBUG

        if logLevel == 'WARN':
            return logging.WARN

        if logLevel == 'ERROR':
            return logging.ERROR

        if logLevel == 'CRITICAL':
            return logging.CRITICAL

        if logLevel == 'INFO':
            return logging.INFO

        return None

    def configure_log(self, args):
        """
        Configures the logger object that is used
        for logging to both CLI output and file

        returns: An instance of a logger class
        """

        cli_log_verbosity = args['console_log_verbosity']
        file_log_verbosity = args['file_log_verbosity']

        # Console Log Level
        cli_log_level = self.get_log_level_from_string(cli_log_verbosity)
        file_log_level = self.get_log_level_from_string(file_log_verbosity)

        # Creating Logger and Configuring console handler
        logger = logging.getLogger('Bootstrapper')
        logger.setLevel(cli_log_level)
        cli_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - \
                                      %(levelname)s - \
                                       %(message)s')
        cli_handler.setFormatter(formatter)
        logger.addHandler(cli_handler)

        # Checking for the need to create a logging file aswell
        if args['log_file']:
            file_handler = logging.FileHandler(args['log_file'])
            file_handler.setLevel(file_log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    def configure_mongodb(self, **kwargs):
        """
        Configures the MongoDB connection wrapper
        """

        mongo_uri = MongoDBWrapper.build_mongo_uri(**kwargs)
        mongo_wrapper = MongoDBWrapper()
        self._mongo_wrapper = mongo_wrapper
        return mongo_wrapper.connect(mongo_uri, kwargs['database'],
                                     kwargs['seed_collection'])

    def fix_url(self, url):
        """ Fix relative Urls by appending the prefix to them """

        url_prefix = 'https://play.google.com'
        return "{0}{1}".format(url_prefix, url)

    def parse_app_urls(self, page_text):
        """
        Extracts urls out of a HTML search page result,
        taking care of duplicates
        """
        # Set tree for html formatting
        tree = html.fromstring(page_text)

        # Xpath parsing
        xpath = "//div[@class='details']/a[@class='card-click-target' and \
                @tabindex='-1' and @aria-hidden='true']"
        urls = tree.xpath(xpath)

        # Sanity check
        if urls is None or len(urls) == 0:
            yield None

        # Go on each node looking for urls
        for node in urls:
            if "href" in node.attrib and "details?id=" in node.attrib["href"]:
                url = node.attrib["href"]

                # Duplicates Check
                if url not in self._parsed_urls:
                    self._parsed_urls.add(url)
                    yield self.fix_url(url)

    def start_bootstrapping(self):
        """
        Main Method - Iterates over all categories, keywords,
                      and misc words, scrapes the urls of all the
                      search results and store them into mongodb
        """

        args_parser = self.get_arguments_parser()
        args = vars(args_parser.parse_args())

        self._logger = self.configure_log(args)

        if not self.configure_mongodb(**self._params):
            self._logger.fatal('Error configuring MongoDB')
            sys.exit(errno.ECONNREFUSED)

        # Loads different "seed" terms from the input xml file received
        bs_seed = BootstrappingSeed.Seed(args['bootstrapping-terms'])
        bs_seed.initialize_seed_class()

        # Request for each top level category
        for top_level_category in bs_seed._top_level_categories:
            self.crawl_category(top_level_category)

    def crawl_category(self, category):
        """
        Executes a GET request for the url of the category received
        and paginates through all the results
        :return:
        """

        category_url = category[1]
        category_name = category[0]

        response = requests.get(category_url,
                                headers={'content-type':
                                         'text/html; charset=UTF-8',
                                         'Accept-Language':
                                         'en-US,en;q=0.6,en;q=0.4,es;q=0.2'})

        self._logger.info('Parsing Category : %s' % category_name)

        # Parse page to get urls of other apps
        urls = self.parse_app_urls(response.text)

        # Adding Urls to MongoDB
        for url in urls:
            self._mongo_wrapper.insert_on_queue(url)

# Starting Point
if __name__ == "__main__":
    bootstrapper = Bootstrapper()
    bootstrapper.start_bootstrapping()
