"""
    Bootstrapping Seed Class:

    Implementation of a "container" of words, terms and links that will be
    used by the 'bootstrapper.py' class in order to build a seed collection
    of apps to process.

    The more diverse the 'search terms' in this class are, the better the
    seed collection will be, since different sets of apps will be reached
    based on this terms.

    E.G: A search for 'Games' and 'Arcade' may lead to two different sets
    of apps, but the intersection between the two groups may be fairly large,
    where as terms such as 'Games' and 'GPS' will lead to two groups with
    a much smaller intersection of apps

"""

import xml.etree.ElementTree as xml_parser
import string


class Seed:

    def __init__(self, terms_file_path):
        self._top_level_categories = []
        self._characters = []
        self._app_categories = []
        self._misc_words = []
        self._country_names = []
        self._xml_map = xml_parser.parse(terms_file_path)

    def initialize_seed_class(self):
        """
        Initializes all the internal collections of links, terms and words used
        for bootstrapping the seed collection of apps.

        Sub methods are used for initializing each individual collection
        """
        self._init_top_level_categories()
        self._init_characters()
        self._init_categories()
        self._init_misc_terms()
        self._init_countries()

    def _init_top_level_categories(self):
        """
        Initializes the collections of 'Top Level Categories' such as
        "Trending", "Top Free" and so forth.

        This data should be placed on the 'top_level_categories' section of
        the input XML file
        """

        # Building tuples based on the xml tags of interest
        for tl_element in self._xml_map.getroot().iter('tl_category'):
            name = tl_element.get('name')
            link = tl_element.text
            self._top_level_categories.append((name, link))

    def _init_characters(self):
        """Initializes a list of all the characters from 'a' to 'z'"""

        [self._characters.append(x) for x in string.ascii_lowercase]

    def _init_categories(self):
        """
        Initializes the collection of categories names

        This data should be placed on the 'categories' section of the
        input XML file
        """

        for category in self._xml_map.getroot().iter('category'):
            self._app_categories.append(category.text)

    def _init_misc_terms(self):
        """
        Initializes the collections of 'Misc Terms' that should contain
        terms that will help increase the reach of the scrapper, by being
        different than the usual ones.

        This data should be placed on the 'misc_terms' section of the
        input XML file
        """

        for category in self._xml_map.getroot().iter('misc'):
            self._misc_words.append(category.text)

    def _init_countries(self):
        """
        Initializes the collections of 'Country Names'.

        This data should be placed on the 'countries' section of the
        input XML file
        """

        for category in self._xml_map.getroot().iter('country'):
            self._country_names.append(category.text)

    def get_words(self):
        words = self._characters
        words.extend(self._app_categories)
        words.extend(self._misc_words)
        words.extend(self._country_names)

        for word in words:
            yield  word