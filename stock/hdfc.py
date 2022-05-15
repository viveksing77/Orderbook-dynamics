from kiteconnect import KiteTicker
from pprint import pprint
import sqlite3

key = 'knbbvqmtix5j6h4z'
access_token = '5O073QbSnKHbCJnHvGyW07Y5uxCBKXSr'
secret ='76kli5jy8kcdgv365py91mifm2ocs010'

kws = KiteTicker( key , access_token)

conn = sqlite3.connect("HDFC.db",check_same_thread=False)
c = conn.cursor()

def create_table():
    c.execute('CREATE TABLE IF NOT EXISTS orderbook( timestamp TEXT, buy TEXT, sell TEXT, last_price TEXT, last_quantity TEXT , last_trade_time TEXT, change TEXT,  average_price TEXT,  buy_quantity TEXT, sell_quantity TEXT, ohlc TEXT, agg_vol TEXT )')
create_table()


def on_ticks( ws , ticks ):
    timestamp = ticks[0]['timestamp']
    
    depth = ticks[0]['depth']
    
    buy = ticks[0]['depth']['buy']

    sell = ticks[0]['depth']['sell']

    last_price = ticks[0]['last_price']

    last_quantity = ticks[0]['last_quantity']

    last_trade_time = ticks[0]['last_trade_time']

    change  = ticks[0]['change']

    average_price =  ticks[0]['average_price']

    buy_quantity =   ticks[0]['buy_quantity']

    sell_quantity =  ticks[0]['sell_quantity']

    ohlc = ticks[0]['ohlc']

    agg_vol = ticks[0]['volume']
    pprint(ticks)
    c.execute("INSERT INTO orderbook ( timestamp , buy , sell , last_price , last_quantity  , last_trade_time , change ,  average_price ,  buy_quantity , sell_quantity , ohlc , agg_vol )VALUES (?, ?, ?, ?, ? , ?, ?, ?, ?, ? , ?, ? )",
    (str(timestamp) , str(buy), str(sell) , str(last_price) , str(last_quantity) , str(last_trade_time) , str(change), str(average_price) , str(buy_quantity) , str(sell_quantity) , str(ohlc) , str(agg_vol)))
    conn.commit()

def on_connect( ws , response ):
    ws.subscribe( [340481] )

    ws.set_mode(ws.MODE_FULL, [340481] )

def on_close( ws , code ,reason):
    ws.stop()

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.connect()

'''
[{'tradable': True, 'mode': 'full', 'instrument_token': 341249, 'last_price': 907.35, 'last_quantity': 2, 'average_price': 940.9, 'volume': 17391140, 'buy_quantity': 490687, 'sell_quantity': 771449, 'ohlc': {'open': 985.0, 'high': 993.0, 'low': 900.1, 'close': 975.1}, 'change': -6.948005332786381, 'last_trade_time': datetime.datetime(2020, 3, 18, 14, 22, 51), 'oi': 0, 'oi_day_high': 0, 'oi_day_low': 0, 'timestamp': datetime.datetime(2020, 3, 18, 14, 22, 51), 'depth': {'buy': [{'quantity': 601, 'price': 907.2, 'orders': 6}, {'quantity': 21, 'price': 907.15, 'orders': 1}, {'quantity': 4, 'price': 907.1, 'orders': 2}, {'quantity': 347, 'price': 907.05, 'orders': 3}, {'quantity': 746, 'price': 907.0, 'orders': 12}], 'sell': [{'quantity': 12, 'price': 907.35, 'orders': 1}, {'quantity': 664, 'price': 907.4, 'orders': 6}, {'quantity': 3, 'price': 907.45, 'orders': 2}, {'quantity': 90, 'price': 907.5, 'orders': 2}, {'quantity': 2, 'price': 907.55, 'orders': 1}]}}]


[{'average_price': 944.27,
  'buy_quantity': 498521,
  'change': -4.835401497282328,
  'depth': {'buy': [{'orders': 1, 'price': 927.9, 'quantity': 4},
                    {'orders': 1, 'price': 927.85, 'quantity': 5},
                    {'orders': 2, 'price': 927.8, 'quantity': 27},
                    {'orders': 1, 'price': 927.7, 'quantity': 100},
                    {'orders': 1, 'price': 927.55, 'quantity': 2}],
            'sell': [{'orders': 1, 'price': 927.95, 'quantity': 9},
                     {'orders': 5, 'price': 928.0, 'quantity': 576},
                     {'orders': 1, 'price': 928.4, 'quantity': 1},
                     {'orders': 2, 'price': 928.45, 'quantity': 27},
                     {'orders': 1, 'price': 928.55, 'quantity': 116}]},
  'instrument_token': 341249,
  'last_price': 927.95,
  'last_quantity': 22,
  'last_trade_time': datetime.datetime(2020, 3, 18, 13, 46, 35),
  'mode': 'full',
  'ohlc': {'close': 975.1, 'high': 993.0, 'low': 900.1, 'open': 985.0},
  'oi': 0,
  'oi_day_high': 0,
  'oi_day_low': 0,
  'sell_quantity': 735880,
  'timestamp': datetime.datetime(2020, 3, 18, 13, 46, 35),
  'tradable': True,
  'volume': 15441212}]

'''