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
        "Score.OneStars":"//div[@class='rating-histogram']/div[@class='rating-bar-container one']/span[@class='bar-number']/text()"
    }



class parser:

    def parse_app_data(self, html):

        # Dictionary to Hold App's data
        app_data = dict()

        # Loading Html
        html_map = lxml.html.fromstring(html)

        # Reaching Useful Data
        app_data['Name'] = self.extract_node_text(html_map, 'Name')
        app_data['CoverImgUrl'] = self.extract_node_text(html_map, 'CoverImgUrl')
        app_data['Screenshots'] = self.extract_node_text(html_map, 'Screenshots', True)
        app_data['Developer'] = self.extract_node_text(html_map, 'Developer')
        app_data['IsTopDeveloper'] = self.extract_node_text(html_map, 'IsTopDeveloper') is not None
        app_data['DeveloperURL'] = self.extract_node_text(html_map, 'DeveloperURL')
        app_data['Description'] = "\n".join(self.extract_node_text(html_map, 'Description', True))
        app_data['WhatsNew'] = "\n".join(self.extract_node_text(html_map, 'WhatsNew', True))
        app_data['HaveInAppPurchases'] = self.extract_node_text(html_map, 'HaveInAppPurchases') is not None

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
            app_data['Price'] = 0.0
        else:
            app_data['IsFree'] = False
            tmp_value = "".join([digit for digit in tmp_value if str.isdigit(digit) or digit in ['.', ',']])
            app_data['Price'] = Decimal(tmp_value.replace(',', '.'))

        # 2 - Category
        tmp_value = self.extract_node_text(html_map, 'Category')

        if '/' in tmp_value:
            app_data['Category'] = tmp_value.split('/')[-1]
        else:
            app_data['Category'] = tmp_value

        # 3 - Reviewers
        tmp_value = self.extract_node_text(html_map, 'Reviewers', True)
        tmp_value = tmp_value[0].strip('(').strip(')').replace(',','').replace('.', '')
        app_data['Reviewers'] = int(tmp_value)

        return app_data

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
