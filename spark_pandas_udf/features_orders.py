# end-to-end spark!
import time
import pandas as pd
import numpy as np
import sparkdfutils as utils


def get_orderbook(ordersdf):
    '''return table of orders agg by side, price'''
    bk = ordersdf.groupby(['side', 'price']).agg({'book_change': 'sum'})
    bk = bk.withColumnRenamed('sum(book_change)', 'quantity')
    return bk.filter('quantity != 0').orderBy('price')


def get_touch(bkdf):
    '''return touch of orderbook'''
    if bkdf.count() > 0
        maxbid = bkdf.filter('''side = 'Buy' ''').agg({'price': 'max'})
        maxbid = maxbid.collect()[0]['max(price)']
        minask = bkdf.filter('''side = 'Sell' ''').agg({'price': 'min'})
        minask = minask.collect()[0]['min(price)']
        return float(maxbid), float(minask)
    else:
        raise ValueError('bkdf.count() == 0')


class Book(object):

    def __init__(self, ordersdf):
        self.bookdf = get_orderbook(ordersdf)
        self.touch = get_touch(bkdf)


    def depthview(self, viewpct=.8, plot=True):
        '''plot depthview for orders with prices in interval
        [viewpct*maxbid, (1.+(1.-viewpct))*minask] if plot==True,
        otherwise return trade-able amounts.
        '''
        maxbid, minask = self.touch
        plotlow, plothigh = viewpct*maxbid, (1.+(1.-viewpct))*minask

        bk = self.bookdf.filter('price BETWEEN %f AND %f' % (plowlow, plothigh))
        bk = bk.toPandas().astype({'price': float, 'quantity': float})
        isbuy = bk['side'] == 'Buy'
        issell = bk['side'] == 'Sell'
        buycum = np.cumsum(bk.loc[isbuy, 'quantity'].values[::-1])[::-1]
        sellcum = np.cumsum(bk.loc[issell, 'quantity'].values)

        out = {'bidp': bk.loc[isbuy, 'price'], 'bidq': buycum,
               'askp': bk.loc[issell, 'price'], 'askq': sellcum}

        if plot:
            plt.plot(out['bidp'], out['bidq'], color='green', alpha=.5)
            plt.plot(out['askp'], out['askq'], color='red', alpha=.5)
            plt.title('Depth View')
            plt.show()
        else:
            return out




def get_dailyorders(symbol, date_string, venue='TSX'):
    '''return table of orders'''
    s = '''SELECT
            book_change, 
            side, 
            price,
            reason,
            time
        FROM orderbook_tsx 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
            AND price > 0
            AND venue = '%s'
        ORDER BY time ASC'''
    sargs = (symbol, date_string, venue)
    return spark.sql(s%sargs)


if __name__ == '__main__':

    symbol = 'TD'
    date_string = '2019-02-04'

    df = get_dailyorders(symbol, date_string)
    bkdf = get_orderbook(df)
    get_touch(bkdf)