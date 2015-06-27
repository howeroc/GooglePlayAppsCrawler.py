import logging
import argparse

def get_arguments_parser():
    parser = argparse.ArgumentParser(description='Bootstrapping phase of the Google Play Store Crawler')

    # All arguments start with "-", hence, they are all handled as optional
    parser.add_argument('--console-log-verbosity', type=str, choices=['INFO', 'DEBUG', 'WARN', 'ERROR', 'CRITICAL'],
                        help='Log Verbosity Level (default=INFO)', default='INFO')

    parser.add_argument('--file-log-verbosity', type=str, choices=['INFO', 'DEBUG', 'WARN', 'ERROR', 'CRITICAL'],
                        help='Log Verbosity Level (default=ERROR)', default='ERROR')

    parser.add_argument('--log-file', type=str, help='Path of the output log file (default is console-only logging)')

    return parser


def get_log_level_from_string(logLevel):
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


def configure_log(args):
    console_log_verbosity = args['console_log_verbosity']
    file_log_verbosity = args['file_log_verbosity']

    # Console Log Level
    console_log_level = get_log_level_from_string(console_log_verbosity)
    file_log_level = get_log_level_from_string(file_log_verbosity)

    # Creating Logger and Configuring console handler
    logger = logging.getLogger('Bootstrapper')
    logger.setLevel(console_log_level)
    cli_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    cli_handler.setFormatter(formatter)
    logger.addHandler(cli_handler)

    # Checking for the need to create a logging file aswell
    if args['log_file']:
        file_handler = logging.FileHandler(args['log_file'])
        file_handler.setLevel(file_log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def start_bootstrapping():
    args_parser = get_arguments_parser()
    args = vars(args_parser.parse_args())

    logger = configure_log(args)

if __name__ == "__main__":
    start_bootstrapping()
