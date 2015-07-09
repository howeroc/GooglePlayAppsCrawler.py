import logging
import argparse
import BootstrappingSeed
import requests
import sys
import errno
import re as regex
import random
import time
from lxml import html
from shared.MongoWrapper import MongoDBWrapper

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

    def load_proxies(self, args):
        """
        Loads HTTP Proxies out of a file and
        returns the list of proxies loaded
        in it's proper 'request-form'
        """
        proxy_format = 'https://{0}:{1}@{2}:{3}/'
        proxies = []

        proxies_reader = args['proxies_path']
        for line in proxies_reader:
            server, port, username, password = line.split(':')
            proxies.append(proxy_format.format(username,
                                               password.replace('\n', ''),
                                               server,
                                               port))

        return proxies

    def get_proxy(self):
        """
        Returns a dictionary with a random loaded proxy
        """
        if self._proxies:
            return {'https': random.choice(self._proxies)}
        return None

    def sleep(self, errors=0):
        """
        Single point of "Process Sleep"
        """
        if errors != 0:
            time.sleep(random.randint(2,3))
        elif errors >= 8:
            time.sleep(60 * 20) # 20 Minutes Nap
        else:
            #Calculating next wait time ( 2 ^ errors, seconds)
            time.sleep(2 ** errors)

    def configure_mongodb(self, **kwargs):
        """
        Configures the MongoDB connection wrapper
        """

        mongo_uri = MongoDBWrapper.build_mongo_uri(**kwargs)
        mongo_wrapper = MongoDBWrapper()
        self._mongo_wrapper = mongo_wrapper
        return mongo_wrapper.connect(mongo_uri, kwargs['database'],
                                     kwargs['seed_collection'])

    def assemble_category_post_data(self, multiplier, base):
        """ Creates a Postdata body based on the arguments received """

        post_data = {
                     'start': multiplier*base,
                     'num': base,
                     'numChildren': 0,
                     'ipf': 1,
                     'xhr' : 1}

        return post_data

    def assemble_word_search_post_data(self, page_token=None):
        """
        Assembles the Postdata for the 'Search' request
        based on the Token received. The first request has a fix postdata
        while the next ones must have different page_tokens, parsed
        out of the html page

        Positional Argument:
        - page_token : Token extracted from the HTML response
        """
        if not page_token:
            return 'ipf=1&xhr=1'

        return 'start=0&num=0&numChildren=0&pagTok={0}&ipf=1&xhr=1'\
               .format(page_token)

    def assemble_post_url(self, word):
        """ Format Search Url based on the word received """
        return 'https://play.google.com/store/search?q={0}&c=apps'.format(word)

    def normalize_page_token(self, page_token):
        """
        Fixes the parsed page token in order to reach the format
        needed for the request to work
        """
        page_token_str = page_token.replace(':S:', '%3AS%3A')
        page_token_str = page_token_str.replace('\\42', '')
        page_token_str = page_token_str.replace('\\u003d', '')
        page_token_str = page_token_str.replace(',', '')
        return page_token_str

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
                yield self.fix_url(url)

    def crawl_category(self, category):
        """
        Executes a GET request for the url of the category received.

        Paginates through all the results found and
        store the unique urls into the MongoDB seed
        collection
        """

        category_url = category[1]
        category_name = category[0]

        self._logger.info('Scraping links of Category : %s' % category_name)
        parsed_urls = set()

        headers={'Host': 'play.google.com',
                 'Origin': 'https://play.google.com',
                 'Content-type':
                 'application/x-www-form-urlencoded;charset=UTF-8',
                 'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64)'
                               ' AppleWebKit/537.36 (KHTML, like Gecko)'
                               ' Chrome/43.0.2357.130 Safari/537.36',
                 'Accept-Language':'en-US,en;q=0.6,en;q=0.4,es;q=0.2'}

        http_errors = 0
        while http_errors <= self._args['max_errors']:

            response = requests.get(category_url,
                                    headers,
                                    verify=self._verify_certificate,
                                    proxies=self.get_proxy())

            if response.status_code != requests.codes.ok:
                http_errors+=1
                #self.sleep(http_errors)
                self._logger.critical('Error [%d] on Response for : %s'
                                      % (response.status_code, category_name))
            else:
                for url in self.parse_app_urls(response.text):
                    self._mongo_wrapper.insert_on_queue(url)
                    parsed_urls.add(url)

                break # Response worked

        # Paging through results
        base_skip = 60
        current_multiplier = 1

        while http_errors <= self._args['max_errors']:

            post_data = self.assemble_category_post_data(current_multiplier,
                                                         base_skip)

            response = requests.post(category_url + '?authuser=0',
                                     data = post_data,
                                     headers=headers,
                                     verify=self._verify_certificate,
                                     proxies=self.get_proxy())

            if response.status_code != requests.codes.ok:
                http_errors+=1
                #self.sleep(http_errors)
                self._logger.critical('Error [%d] on Response for : %s'
                                      % (response.status_code, category_name))
            else:
                for url in self.parse_app_urls(response.text):
                    if url in parsed_urls: # Duplicate Check, Means End of Loop
                        return

                    parsed_urls.add(url)
                    self._mongo_wrapper.insert_on_queue(url)
                    #self.sleep()

            current_multiplier+=1

    def crawl_by_search_word(self, word):
        """
        Simulates an app search on the play store, using the
        word argument as the term to be searched.

        Paginates through all the results found and
        store the unique urls into the MongoDB seed
        collection
        """

        self._logger.info('Scraping links of Word : %s' % word)
        parsed_urls = set()

        # Compiling regex used for parsing page token
        page_token_regex = regex.compile(r"GAEi+.+\:S\:.{11}\\42,")

        headers={'Host': 'play.google.com',
                 'Origin': 'https://play.google.com',
                 'Content-type':
                 'application/x-www-form-urlencoded;charset=UTF-8',
                 'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64)'
                               ' AppleWebKit/537.36 (KHTML, like Gecko)'
                               ' Chrome/43.0.2357.130 Safari/537.36',
                 'Accept-Language':'en-US,en;q=0.6,en;q=0.4,es;q=0.2'}

        post_url = self.assemble_post_url(word)
        post_data = self.assemble_word_search_post_data()

        http_errors = 0
        while http_errors <= self._args['max_errors']:

            response = requests.post(post_url,
                                    data=post_data,
                                    headers=headers,
                                    verify=self._verify_certificate,
                                    proxies=self.get_proxy())

            if response.status_code != requests.codes.ok:
                http_errors+=1
                #self.sleep(http_errors)
                self._logger.critical('Error [%d] on Response for : %s'
                                      % (response.status_code, word))
            else:
                for url in self.parse_app_urls(response.text):
                    self._mongo_wrapper.insert_on_queue(url)
                    parsed_urls.add(url)

                break # Response worked

        # Paging Through Results
        while http_errors <= self._args['max_errors']:

            page_token = page_token_regex.search(response.text)

            if not page_token:
                self._logger.fatal("Couldn't find page token")
                break

            page_token = self.normalize_page_token(page_token.group())
            post_data = self.assemble_word_search_post_data(page_token)

            response = requests.post(post_url,
                                     data=post_data,
                                     headers=headers,
                                     verify=self._verify_certificate,
                                     proxies=self.get_proxy())

            if response.status_code != requests.codes.ok:
                http_errors+=1
                #self.sleep(http_errors)
                self._logger.critical('Error [%d] on Response for : %s'
                                      % (response.status_code, word))
            else:
                for url in self.parse_app_urls(response.text):
                    if url in parsed_urls:
                        return

                    self._mongo_wrapper.insert_on_queue(url)
                    parsed_urls.add(url)
                    #self.sleep()

    def start_bootstrapping(self):
        """
        Main Method - Iterates over all categories, keywords,
                      and misc words, scrapes the urls of all the
                      search results and store them into mongodb
        """

        args_parser = self.get_arguments_parser()
        self._args = vars(args_parser.parse_args())

        self._logger = self.configure_log(self._args)

        if not self.configure_mongodb(**self._params):
            self._logger.fatal('Error configuring MongoDB')
            sys.exit(errno.ECONNREFUSED)

        # Loads different "seed" terms from the input xml file received
        bs_seed = BootstrappingSeed.Seed(self._args['bootstrapping-terms'])
        bs_seed.initialize_seed_class()

        # Checking for proxies
        if self._args['proxies_path']:
            self._proxies = self.load_proxies(self._args)
            print 'Loaded Proxies : %d' % len(self._proxies)
        else:
            self._proxies = None

        # if "Debug Http" is set to true, "verify" must be "false"
        self._verify_certificate = not self._args['debug_https']

        # Creating MongoDB Index on Seed Collection
        self._mongo_wrapper.ensure_index('Url')

        # Request for each top level category
        for top_level_category in bs_seed._top_level_categories:
            self.crawl_category(top_level_category)

        # Simulating searches for specific words
        for word in bs_seed.get_words():
            self.crawl_by_search_word(word)


# Starting Point
if __name__ == "__main__":

    requests.packages.urllib3.disable_warnings()
    bootstrapper = Bootstrapper()
    bootstrapper.start_bootstrapping()
