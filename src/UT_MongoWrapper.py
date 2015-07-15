__author__ = 'Rafael'

import unittest
from shared.MongoWrapper import MongoDBWrapper

class MongoWrapperTests(unittest.TestCase):

    _test_app_url = 'unit_test_url'


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
        params['server'] = 'mobiledata.bigdatacorp.com.br'
        params['port'] = '21766'
        params['database'] = 'MobileAppsData'
        params['username'] = 'GitHubCrawlerUser'
        params['password'] = 'g22LrJvULU5' # Wrong password
        params['seed_collection'] = 'Python_test'
        params['auth_database'] = 'MobileAppsData'
        params['write_concern'] = True

        mongo_uri = MongoDBWrapper.build_mongo_uri(**params)

        mongo_wrapper = MongoDBWrapper()

        is_connected = mongo_wrapper.connect(mongo_uri,
                                                params['database'],
                                                params['seed_collection'])

        self.assertFalse(is_connected,
                         'Connection success when it should not be ok.')



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

        if is_connected:
            mongo_wrapper.insert_on_queue(self._test_app_url)

            # Find it on Mongo
            query = {'_id': self._test_app_url}
            self.assertTrue(mongo_wrapper._collection.find_one(query),
                            'Insert did not work.')

        else:
            self.fail('Connection problem, verify connection before insert.')



if __name__ == '__main__':
    unittest.main()
