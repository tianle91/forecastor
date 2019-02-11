from Book import Book

def dailyorders(symbol, date_string, venue):
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
            AND venue = '%s'
            AND price > 0
            AND price < 99999
        ORDER BY time ASC'''
    sargs = (symbol, date_string, venue)
    return spark.sql(s%sargs)


def orderbook(ordersdf, timestamp):
    '''return table of orderbook'''
    df = ordersdf.filter('''time <= %s''' % (str(timestamp)))
    bk = df.groupby(['side', 'price']).agg({'book_change': 'sum'})
    bk = bk.withColumnRenamed('sum(book_change)', 'quantity')
    return bk.orderby('price')


tradingtimes = 

features = {}

features['orderbook'] = 