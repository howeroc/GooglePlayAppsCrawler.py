import logging
import random
import time
from MongoWrapper import MongoDBWrapper

class Utils:
    """
    This class is used to share useful code between the 'Bootstrapper' and the
    'Worker' layers of this project. Tasks in common such as Proxies handling,
    logging and MongoDB configuration live here to maximize code
    maintainability
    """

    @staticmethod
    def get_log_level_from_string(logLevel):
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

    @staticmethod
    def configure_log(args):
        """
        Configures the logger object that is used
        for logging to both CLI output and file

        returns: An instance of a logger class
        """

        cli_log_verbosity = args['console_log_verbosity']
        file_log_verbosity = args['file_log_verbosity']

        # Console Log Level
        cli_log_level = Utils.get_log_level_from_string(cli_log_verbosity)
        file_log_level = Utils.get_log_level_from_string(file_log_verbosity)

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

    @staticmethod
    def configure_mongodb(caller_class, **kwargs):
        """
        Configures the MongoDB connection wrapper
        """

        mongo_uri = MongoDBWrapper.build_mongo_uri(**kwargs)
        mongo_wrapper = MongoDBWrapper()
        caller_class._mongo_wrapper = mongo_wrapper
        return mongo_wrapper.connect(mongo_uri, kwargs['database'],
                                     kwargs['seed_collection'])

    @staticmethod
    def load_proxies(args):
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

    @staticmethod
    def get_proxy(caller_class):
        """
        Returns a dictionary with a random loaded proxy
        """
        if caller_class._proxies:
            return {'https': random.choice(caller_class._proxies)}
        return None

    @staticmethod
    def sleep(errors=0):
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

class HTTPUtils:
    headers={'Host': 'play.google.com',
                 'Origin': 'https://play.google.com',
                 'Content-type':
                 'application/x-www-form-urlencoded;charset=UTF-8',
                 'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64)'
                               ' AppleWebKit/537.36 (KHTML, like Gecko)'
                               ' Chrome/43.0.2357.130 Safari/537.36',
                 'Accept-Language':'en-US,en;q=0.6,en;q=0.4,es;q=0.2'}