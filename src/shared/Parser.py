import lxml

class XPath:

    xPaths = {
        "Name" : "//div[@class='info-container']/div[@class='document-title' and @itemprop='name']/div/text()",
        "CoverImgUrl": "//div[@class='details-info']/div[@class='cover-container']/img[@class='cover-image']/@src",
        "Screenshots": "//div[@class='thumbnails']//img[contains(@class,'screenshot')]/@src",
        "Category": "//div/a[@class='document-subtitle category']/@href",
        "Developer": "//div[@class='info-container']/div[@itemprop='author']/a/span[@itemprop='name']/text()",
        "IsTopDeveloper": "//meta[@itemprop='topDeveloperBadgeUrl']/@itemprop"
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

        tmp_value = self.extract_node_text(html_map, 'Category')

        if '/' in tmp_value:
            app_data['Category'] = tmp_value.split('/')[-1]
        else:
            app_data['Category'] = tmp_value






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
            return list(x.strip() for x in set(node))
