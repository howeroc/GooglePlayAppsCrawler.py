import lxml
from decimal import Decimal

class XPath:

    xPaths = {
        "Name" : "//div[@class='info-container']/div[@class='document-title' and @itemprop='name']/div/text()",
        "CoverImgUrl": "//div[@class='details-info']/div[@class='cover-container']/img[@class='cover-image']/@src",
        "Screenshots": "//div[@class='thumbnails']//img[contains(@class,'screenshot')]/@src",
        "Category": "//div/a[@class='document-subtitle category']/@href",
        "Developer": "//div[@class='info-container']/div[@itemprop='author']/a/span[@itemprop='name']/text()",
        "IsTopDeveloper": "//meta[@itemprop='topDeveloperBadgeUrl']/@itemprop",
        "DeveloperURL": "//div[@class='info-container']/div[@itemprop='author']/meta[@itemprop='url']/@content",
        "Price": "//span[@itemprop='offers' and @itemtype='http://schema.org/Offer']/meta[@itemprop='price']/@content",
        "Reviewers": "//div[@class='header-star-badge']/div[@class='stars-count']/text()",
        "Description": "//div[@class='show-more-content text-body' and @itemprop='description']/div/text()|//div[@class='show-more-content text-body' and @itemprop='description']/div/p/text()",
        "WhatsNew": "//div[@class='recent-change']/text()",
        "HaveInAppPurchases": "//div[@class='title' and contains(text(),'app')]/following-sibling::div/text()",
        "Score.Count": "//div[@class='rating-box']/div[@class='score-container']/meta[@itemprop='ratingValue']/@content",
        "Score.FiveStars":"//div[@class='rating-histogram']/div[@class='rating-bar-container five']/span[@class='bar-number']/text()",
        "Score.FourStars":"//div[@class='rating-histogram']/div[@class='rating-bar-container four']/span[@class='bar-number']/text()",
        "Score.ThreeStars":"//div[@class='rating-histogram']/div[@class='rating-bar-container three']/span[@class='bar-number']/text()",
        "Score.TwoStars":"//div[@class='rating-histogram']/div[@class='rating-bar-container two']/span[@class='bar-number']/text()",
        "Score.OneStars":"//div[@class='rating-histogram']/div[@class='rating-bar-container one']/span[@class='bar-number']/text()",
        "LastUpdateDate": "//div[@class='meta-info']/div[@itemprop='datePublished']/text()",
        "CurrentVersion": "//div[@class='content' and @itemprop='softwareVersion']/text()",
        "Instalations": "//div[@class='content' and @itemprop='numDownloads']/text()",
        "ContentRating": "//div[@class='content' and @itemprop='contentRating']/text()",
        "MinimumOSVersion": "//div[@class='content' and @itemprop='operatingSystems']/text()",
        "DeveloperUrls": "//div[@class='content contains-text-link']/a[@class='dev-link']",
        "PhysicalAddress": "//div[@class='content physical-address']/text()",
        "RelatedApps": "//div[@class='card-content id-track-click id-track-impression']/a[@class='card-click-target']/@href"
    }



class parser:

    def parse_app_data(self, html):
        """
        Extracts relevant data out of the html received as argument

        :return: Dictionary mapping the app data
        """

        # Dictionary to Hold App's data
        app_data = dict()

        # Loading Html
        html_map = lxml.html.fromstring(html)

        description = self.extract_node_text(html_map, 'Description', True)
        whats_new = self.extract_node_text(html_map, 'WhatsNew', True)
        if description is None:
            description=''
        else:
            description = "\n".join(description)
        if whats_new is None:
            whats_new=''
        else:
            whats_new = "\n".join(whats_new)

        # Reaching Useful Data
        app_data['Name'] = self.extract_node_text(html_map, 'Name')
        app_data['CoverImgUrl'] = self.extract_node_text(html_map, 'CoverImgUrl')
        app_data['Screenshots'] = self.extract_node_text(html_map, 'Screenshots', True)
        app_data['Developer'] = self.extract_node_text(html_map, 'Developer')
        app_data['IsTopDeveloper'] = self.extract_node_text(html_map, 'IsTopDeveloper') is not None
        app_data['DeveloperURL'] = self.extract_node_text(html_map, 'DeveloperURL')
        app_data['Description'] = description
        app_data['WhatsNew'] = whats_new
        app_data['HaveInAppPurchases'] = self.extract_node_text(html_map, 'HaveInAppPurchases') is not None
        app_data['LastUpdateDate'] = self.extract_node_text(html_map, 'LastUpdateDate')
        app_data['CurrentVersion'] = self.extract_node_text(html_map, 'CurrentVersion')
        app_data['Instalations'] = self.extract_node_text(html_map, 'Instalations')
        app_data['ContentRating'] = self.extract_node_text(html_map, 'ContentRating')
        app_data['MinimumOSVersion'] = self.extract_node_text(html_map, 'MinimumOSVersion')

        address = self.extract_node_text(html_map, 'PhysicalAddress')
        if address:
            app_data['PhysicalAddress'] = address.replace('\n', ' ')
        else:
            app_data['PhysicalAddress'] = None

        # Parsing App's Score
        score = dict()

        score['Count'] = self.extract_node_as_decimal(html_map, 'Score.Count')
        score['FiveStars'] = self.extract_node_as_integer(html_map, 'Score.FiveStars')
        score['FourStars'] = self.extract_node_as_integer(html_map, 'Score.FourStars')
        score['ThreeStars'] = self.extract_node_as_integer(html_map, 'Score.ThreeStars')
        score['TwoStars'] = self.extract_node_as_integer(html_map, 'Score.TwoStars')
        score['OneStars'] = self.extract_node_as_integer(html_map, 'Score.OneStars')

        # Attributes that require special handling to be calculated / scraped

        # 1 - Price and IsFree
        tmp_value = self.extract_node_text(html_map, "Price")

        if tmp_value is None or tmp_value is '0':
            app_data['IsFree'] = True
            app_data['Price'] = 0
        else:
            app_data['IsFree'] = False
            tmp_value = "".join([digit for digit in tmp_value if str.isdigit(digit) or digit in ['.', ',']])
            app_data['Price'] = int(Decimal(tmp_value.replace(',', '.')) * 100) # PRICE IS MULTIPLIED by 100 TO MAKE IT MONGODB FRIENDLY AS AN INT

        # 2 - Category
        tmp_value = self.extract_node_text(html_map, 'Category')

        if '/' in tmp_value:
            app_data['Category'] = tmp_value.split('/')[-1]
        else:
            app_data['Category'] = tmp_value

        # 3 - Reviewers
        tmp_value = self.extract_node_text(html_map, 'Reviewers', True)
        tmp_value = tmp_value[1].replace('(', '').replace(')', '').replace(',','').replace('.', '')
        app_data['Reviewers'] = int(tmp_value)

        # 4 - Developer Urls (Privacy, Email and Website)
        dev_urls = self.extract_developer_urls(html_map)
        app_data['DeveloperEmail'] = dev_urls.get('Email', None)
        app_data['DeveloperWebsite'] = dev_urls.get('Site', None)
        app_data['DeveloperPrivacyPolicy'] = dev_urls.get('Privacy', None)

        return app_data

    def parse_related_apps(self, html):
        # Loading Html
        html_map = lxml.html.fromstring(html)

        # Reaching Useful Data
        xpath = XPath.xPaths['RelatedApps']
        nodes = html_map.xpath(xpath)

        # Appending url prefix to the actual url found within the html
        return map((lambda url: '{0}{1}'.format('https://play.google.com', url)), nodes)

    def extract_node_text(self, map, key, is_list=False):
        """
        Applies the XPath mapped by the "key" received, into the
        map object that contains the html loaded from the response
        """
        if key not in XPath.xPaths:
            return None

        xpath = XPath.xPaths[key]
        node = map.xpath(xpath)

        if not node:
            return None

        if not is_list:
            return node[0].strip()
        else:
            # Distinct elements found
            seen = set()
            return [x for x in node if x not in seen and not seen.add(x)]

    def extract_node_as_decimal(self, map, key, decimal_places = 1):
        """
        Parses the result found within that xpath node,
        as a decimal.

        If no value is found, None is returned
        """
        node = self.extract_node_text(map,key)

        if node:
            return round(Decimal(node), decimal_places)

        return None

    def extract_node_as_integer(self, map, key):
        """
        Parses the result found within that xpath node,
        as an integer.

        If no value is found, None is returned
        """
        node = self.extract_node_text(map,key)

        if node:
            parseable_str = [digit for digit in node if str.isdigit(digit)]
            return int("".join(parseable_str))

        return None

    def extract_developer_urls(self, map):
        """
        Applies the XPath related to Developer Data into the
        map object that contains the html loaded from the response.

        Returns a dictionary with relevant data found about the developer
        """
        xpath = XPath.xPaths['DeveloperUrls']
        nodes = map.xpath(xpath)

        if not nodes:
            return None

        dev_data = dict()

        for tmp_node in nodes:
            if 'MAIL' in tmp_node.text.upper():
                dev_data['Email'] = tmp_node.attrib['href'].replace('mailto:','')
            elif 'SITE' in tmp_node.text.upper():
                dev_data['Site'] = tmp_node.attrib['href']
            else:
                dev_data['Privacy'] = tmp_node.attrib['href'].replace('https://www.google.com/url?q=', '')

        return dev_data