from stem import Signal
from stem.control import Controller

class TorProxy:

    # signal TOR for a new connection
    @staticmethod
    def change_ip():
        with Controller.from_port(port = 9051) as controller:
            controller.authenticate(password="weiphone2011")
            controller.signal(Signal.NEWNYM)

    @staticmethod
    def get_proxy():
        with Controller.from_port(port = 9051) as controller:
            controller.authenticate(password="weiphone2011")
            controller.signal(Signal.NEWNYM)
            proxies = {
                'https': 'socks5://localhost:9050'
            }
            return proxies