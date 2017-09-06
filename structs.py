from collections import namedtuple


class Deal(namedtuple('Deal', ['price', 'quantity', 'side'])):
    def __repr__(self):
        return '\n(p={},q={},{})'.format(*self)


class MarketInfo(namedtuple('MarketInfo', ['exchange', 'volume', 'market',
                                           'data'])):
    def __repr__(self):
        return '-----------\n e={},v={},mkt={},\n d={} \n------------\n'.format(*self)

class ArbOpp:
    def __init__(self,
                 e_from=None, e_to=None, mkt_from=None, mkt_to=None,
                 start_date=None, end_date=None,
                 price_buy=None, price_sell=None,
                 strength=None,
                 why_closed='OPEN'):
        self.e_from = e_from
        self.e_to = e_to
        self.mkt_from = mkt_from
        self.mkt_to = mkt_to
        self.start_date = start_date
        self.end_date = end_date
        self.price_buy = price_buy
        self.price_sell = price_sell
        self.strength = strength
        self.why_closed = why_closed

    def get_key(self):
        return (self.mkt_from, self.mkt_to, self.e_from, self.e_to)

    def get_duration_minutes(self):
        dt = self.end_date - self.start_date
        return dt.total_seconds() // 60

    def get_roi(self):
        return self.price_sell / self.price_buy - 1


    def __repr__(self):
        return ("roi=%.0f%% mkt_from=%s mkt_to=%s e_from=%s e_to=%s "
                "price_buy=%.0f$ price_sell=%.0f$ start_date=%s end_date=%s "
                "strength=%.2f "
                "duration=%s why_closed=%s" %  (self.get_roi() * 100,
                                                self.mkt_from,
                                                self.mkt_to,
                                                self.e_from,
                                                self.e_to,
                                                self.price_buy,
                                                self.price_sell,
                                                self.start_date,
                                                self.end_date,
                                                self.strength,
                                                self.get_duration_minutes(),
                                                self.why_closed))

    def __hash__(self):
        return hash(self.get_key())

    def __eq__(self, other):
        return self.get_key() == other.get_key()
