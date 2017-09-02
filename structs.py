from collections import namedtuple


class Deal(namedtuple('Deal', ['price', 'quantity', 'side'])):
    def __repr__(self):
        return '\n(p={},q={},{})'.format(*self)


class MarketInfo(namedtuple('MarketInfo', ['exchange', 'volume', 'market', 'price', 'deals'])):
    def __repr__(self):
        return '-----------\n e={},v={},mkt={},p={} \n d={} \n------------\n'.format(*self)
