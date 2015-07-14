__author__ = 'Rafael'

import unittest
from shared.MongoWrapper import MongoDBWrapper

class MongoWrapperTests(unittest.TestCase):

    def test_connection_success(self):
        params = {}
        params['server'] = 'mobiledata.bigdatacorp.com.br'
        params['port'] = '21766'
        params['database'] = 'MobileAppsData'
        params['username'] = 'GitHubCrawlerUser'
        params['password'] = 'g22LrJvULU5B'
        params['seed_collection'] = 'Python_test'
        params['auth_database'] = 'MobileAppsData'
        params['write_concern'] = True

        mongo_uri = MongoDBWrapper.build_mongo_uri(**params)

        mongo_wrapper = MongoDBWrapper()
        is_connected = mongo_wrapper.connect(mongo_uri, params['database'],
                                             params['seed_collection'])

        self.assertTrue(is_connected, 'Failed to connect.')

    def test_connection_fail(self):
        params = {}
        params['server'] = 'ERROR'
        params['port'] = '21766'
        params['database'] = 'MobileAppsData'
        params['username'] = 'GitHubCrawlerUser'
        params['password'] = 'g22LrJvULU5B'
        params['seed_collection'] = 'Python_test'
        params['auth_database'] = 'MobileAppsData'
        params['write_concern'] = True

        mongo_uri = MongoDBWrapper.build_mongo_uri(**params)

        mongo_wrapper = MongoDBWrapper()
        is_connected = mongo_wrapper.connect(mongo_uri, params['database'],
                                             params['seed_collection'])

        self.assertFalse(is_connected,
                         'is_connected should be false but received true.')

    def test_insertion_success(self):
        params = {}
        params['server'] = 'mobiledata.bigdatacorp.com.br'
        params['port'] = '21766'
        params['database'] = 'MobileAppsData'
        params['username'] = 'GitHubCrawlerUser'
        params['password'] = 'g22LrJvULU5B'
        params['seed_collection'] = 'Python_test'
        params['auth_database'] = 'MobileAppsData'
        params['write_concern'] = True

        mongo_uri = MongoDBWrapper.build_mongo_uri(**params)

        mongo_wrapper = MongoDBWrapper()
        is_connected = mongo_wrapper.connect(mongo_uri, params['database'],
                                             params['seed_collection'])

        unittest.skipIf(is_connected is False,
                     'Connection failed, insertion cancelled.')

        acknowledged = False

        if is_connected:
            acknowledged = mongo_wrapper._insert('unit_test_url')

        self.assertTrue(acknowledged, 'Insertion test failed.')


if __name__ == '__main__':
    unittest.main()
