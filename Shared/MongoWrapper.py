from pymongo import mongo_client
from pymongo import errors


class MongoDBWrapper:

    def __init__(self):
        """
        Wraps and stores all MongoDB related
        objects and performs top-level operations
        by using pymongo as a proxy class
        """
        pass

    @staticmethod
    def build_mongo_uri(**kwargs):
        """
        Kwargs:
            - server (str) - server address (Default:localhost)

            - port (int) - port number (Default:27017)

            - username (str) - username credential

            - password (str) - password credential

            - auth_database (str) - name of the authentication database

            - write_concern (bool) - Write Concern ( Check at
            http://docs.mongodb.org/manual/core/write-concern/#write-concern)

            - read_secondary (bool) - Read Preference (Default: false)

            - connection_timeout (int) - Time in milliseconds for connection
                                     (Default: 16000)

            - socket_timeout (int) - Time in milliseconds for operations
                                     (Default: 16000)

        """

        uri_builder = []
        uri_builder.append('mongodb://')

        if 'username' in kwargs and 'password' in kwargs:
            uri_builder.append(kwargs['username'])
            uri_builder.append(':')
            uri_builder.append(kwargs['password'])
            uri_builder.append('@')

        uri_builder.append(kwargs.get('server', 'localhost'))
        uri_builder.append(':')
        uri_builder.append(kwargs.get('port', '27017'))
        uri_builder.append('/?')

        if 'auth_database' in kwargs:
            uri_builder.append('authSource=')
            uri_builder.append(kwargs['auth_database'])
            uri_builder.append('&')

        if 'write_concern' in kwargs:
            uri_builder.append('w=1&')
        else:
            uri_builder.append('w=0&')

        if 'read_secondary' in kwargs:
            uri_builder.append('readPreference=secondaryPreferred&')
        else:
            uri_builder.append('readPreference=primaryPreferred&')

        uri_builder.append('connectTimeoutMS=')
        uri_builder.append(kwargs.get('connection_timeout', '16000'))
        uri_builder.append('&socketTimeoutMS=')
        uri_builder.append(kwargs.get('socket_timeout', '16000'))

        return ''.join(uri_builder)

    def connect(self, uri, database, collection=None):
        """
        Positional Arguments:
        - uri (string) - Mongo-compliant connection uri string
        database (string) - name of the database you want to connect to

        Optional Arguments:
        - collection (string) - name of the collection you want to reach
                              (Default: None)
        """

        try:
            self._client = mongo_client.MongoClient(uri)
            self._database = self._client[database]

            # Reaching collection names to check whether the received exists
            print 'Database Status : %s' % self._database.last_status()

            if collection:
                self._collection = self._database[collection]

            return True

        except errors.OperationFailure as e:
            print 'Error Code : %s - Details: %s' % (e.code, e.details)

        return False

    def app_processed(self, app_url, collection=None):
        """
        Checks whether an app (identified by the url) already got processed
        or not.

        Positional Arguments:
        - app_url (str) - Full url of the app to be searched

        Optional Arguments:
        - collection (str) - Name of the collection to be searched
                             (Default: None)


        returns True if the app was found, false otherwise
        """

        query = {'Url': app_url}

        # Checking for the need to use the parameter collection
        if collection is not None:
            return self._database[collection].find_one(query) != None

        return self._collection.find_one(query) != None

    def insert(self, app, collection=None):
        """
        Inserts an app into the default class collection or into
        the specified one, if any.

        Positional Arguments
        - app (Any) - Record to be inserted

        Optional Arguments
        - collection (str) - Name of the collection that the record should be
        added to. (Default: None)

        returns True if the operation worked, False otherwise
        """

        if collection is None:
            return self._collection.insert_one(app).acknowledged

        return self._database[collection].insert_one(app).acknowledged


if __name__ == '__main__':

    print 'Connecting to the database'

    # Dictionary of Connection Parameters
    params = {}
    params['server'] = 'mobiledata.bigdatacorp.com.br'
    params['port'] = '21766'
    params['database'] = 'MobileAppsData'
    params['username'] = 'GitHubCrawlerUser'
    params['password'] = 'g22LrJvULU5B'
    params['seed_collection'] = 'Python_test'
    params['auth_database'] = 'MobileAppsData'
    params['write_concern'] = True

    mongo_uri = MongoDBWrapper.build_mongo_uri(params)

    mongo_wrapper = MongoDBWrapper()
    is_connected = mongo_wrapper.connect(mongo_uri, params['database'],
                                         params['seed_collection'])
