from kiteconnect import KiteTicker
from pprint import pprint
import sqlite3

key = 'knbbvqmtix5j6h4z'
access_token = '5O073QbSnKHbCJnHvGyW07Y5uxCBKXSr'
secret ='76kli5jy8kcdgv365py91mifm2ocs010'

kws = KiteTicker( key , access_token)

conn = sqlite3.connect("SBI.db",check_same_thread=False)
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
    ws.subscribe( [779521] )

    ws.set_mode(ws.MODE_FULL, [779521] )

def on_close( ws , code ,reason):
    ws.stop()

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.connect()