import logging
import argparse
import BootstrappingSeed
import requests
import sys
import errno
import re as regex
from lxml import html
from shared.Utils import Utils
from shared.Utils import HTTPUtils

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
                            help='Path to the xml containing the bootstrapping\
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

        http_errors = 0
        while http_errors <= self._args['max_errors']:

            try:
                response = requests.get(category_url,
                                        HTTPUtils.headers,
                                        verify=self._verify_certificate,
                                        proxies=Utils.get_proxy())

                if response.status_code != requests.codes.ok:
                    http_errors+=1
                    #Utils.sleep(http_errors)
                    self._logger.critical('Error [%d] on Response for : %s'
                                       % (response.status_code, category_name))
                else:
                    for url in self.parse_app_urls(response.text):
                        self._mongo_wrapper.insert_on_queue(url)
                        parsed_urls.add(url)

                    break # Response worked

            except requests.exceptions.SSLError as error:
                print 'SSL_Error : ' + error.errno

        # Paging through results
        base_skip = 60
        current_multiplier = 1

        while http_errors <= self._args['max_errors']:

            post_data = self.assemble_category_post_data(current_multiplier,
                                                         base_skip)
            try:
                response = requests.post(category_url + '?authuser=0',
                                         data = post_data,
                                         headers=HTTPUtils.headers,
                                         verify=self._verify_certificate,
                                         proxies=Utils.get_proxy())

                if response.status_code != requests.codes.ok:
                    http_errors+=1
                    #Utils.sleep(http_errors)
                    self._logger.critical('Error [%d] on Response for : %s'
                                      % (response.status_code, category_name))
                else:
                    for url in self.parse_app_urls(response.text):
                        if url in parsed_urls:
                            return

                        parsed_urls.add(url)
                        self._mongo_wrapper.insert_on_queue(url)
                        #Utils.sleep()

            except requests.exceptions.SSLError as error:
                print 'SSL_Error : ' + error.errno

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

        post_url = self.assemble_post_url(word)
        post_data = self.assemble_word_search_post_data()

        http_errors = 0
        while http_errors <= self._args['max_errors']:

            try:
                response = requests.post(post_url,
                                        data=post_data,
                                        headers=HTTPUtils.headers,
                                        verify=self._verify_certificate,
                                        proxies=Utils.get_proxy())

                if response.status_code != requests.codes.ok:
                    http_errors+=1
                    #Utils.sleep(http_errors)
                    self._logger.critical('Error [%d] on Response for : %s'
                                          % (response.status_code, word))
                else:
                    for url in self.parse_app_urls(response.text):
                        self._mongo_wrapper.insert_on_queue(url)
                        parsed_urls.add(url)

                    break # Response worked

            except requests.exceptions.SSLError as error:
                print 'SSL_Error : ' + error.errno

        # Paging Through Results
        while http_errors <= self._args['max_errors']:

            page_token = page_token_regex.search(response.text)

            if not page_token:
                self._logger.fatal("Couldn't find page token")
                break

            page_token = self.normalize_page_token(page_token.group())
            post_data = self.assemble_word_search_post_data(page_token)

            try:
                response = requests.post(post_url,
                                         data=post_data,
                                         headers=HTTPUtils.headers,
                                         verify=self._verify_certificate,
                                         proxies=Utils.get_proxy())

                if response.status_code != requests.codes.ok:
                    http_errors+=1
                    #Utils.sleep(http_errors)
                    self._logger.critical('Error [%d] on Response for : %s'
                                          % (response.status_code, word))
                else:
                    for url in self.parse_app_urls(response.text):
                        if url in parsed_urls:
                            return

                        self._mongo_wrapper.insert_on_queue(url)
                        parsed_urls.add(url)
                        #Utils.sleep()

            except requests.exceptions.SSLError as error:
                print 'SSL_Error : ' + error.errno

    def start_bootstrapping(self):
        """
        Main Method - Iterates over all categories, keywords,
                      and misc words, scrapes the urls of all the
                      search results and store them into mongodb
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

        # Loads different "seed" terms from the input xml file received
        bs_seed = BootstrappingSeed.Seed(self._args['bootstrapping-terms'])
        bs_seed.initialize_seed_class()

        # Checking for proxies
        if self._args['proxies_path']:
            self._proxies = Utils.load_proxies(self._args)
            print 'Loaded Proxies : %d' % len(self._proxies)
        else:
            self._proxies = None

        # if "Debug Http" is set to true, "verify" must be "false"
        self._verify_certificate = not self._args['debug_https']

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
