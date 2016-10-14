from stem import Signal
from stem.control import Controller
import requests

class TorProxy:
    # signal TOR for a new connection
    def change_ip(self):
        proxies = {'http':  'socks5://127.0.0.1:9050',
                   'https': 'socks5://127.0.0.1:9050'}
        pre_ip = requests.get('http://httpbin.org/ip', proxies=proxies).text
        with Controller.from_port(port = 9051) as controller:
            controller.authenticate(password="weiphone2011")
            controller.signal(Signal.NEWNYM)

        after_ip = requests.get('http://httpbin.org/ip', proxies=proxies).text
        return  pre_ip + after_ip
    @staticmethod
    def get_proxy():
        with Controller.from_port(port = 9051) as controller:
            controller.authenticate(password="weiphone2011")
            controller.signal(Signal.NEWNYM)
        proxies = {'https': 'socks5://127.0.0.1:9050'}
        return proxies
