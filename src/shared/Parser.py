import lxml

class XPath:

    xPaths = {
        "Name" : "//div[@class='info-container']/div[@class='document-title' \
                  and @itemprop='name']/div/text()",

        "CoverImgUrl": "//div[@class='details-info']/div\
                        [@class='cover-container']/img[@class='cover-image']",

        "Screenshots": "//div[@class='thumbnails' ]//img[contains\
                       (@class,'screenshot')]"
    }



class parser:

    def parse_app_data(self, html):

        # Dictionary to Hold App's data
        app_data = dict()

        # Loading Html
        html_map = lxml.html.fromstring(html)

        # Reaching Useful Data
        app_data['Name'] = self.extract_node_text(html_map, 'Name')


    def extract_node_text(self, map, key):

        if key not in XPath.xPaths:
            return None

        xpath = XPath.xPaths[key]
        node = map.xpath(xpath)

        if not node:
            return None

        return node[0]