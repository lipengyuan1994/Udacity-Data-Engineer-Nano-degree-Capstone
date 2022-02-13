import pandas as pd
import ccxt
import time
import os
import datetime
import logging

pd.set_option('expand_frame_repr', False)
pd.options.display.max_columns = None


def save_spot_candle_data_from_exchange(exchange, symbol, time_interval, start_time, path, type):
    """
    this is module is created for Binance
    :param exchange: ccxt
    :param symbol: 'BTC/USDT'
    :param time_interval: K
    :param start_time: '2020-03-16 00:00:00'
    :param path: 
    :param type: s: spot   f:future
    :return:
    """

    limit = None
    if exchange.id == 'huobipro':
        limit = 2000

    df_list = []
    start_time_since = exchange.parse8601(start_time)
    end_time = pd.to_datetime(start_time) + datetime.timedelta(days=1)

    while True:

        df = exchange.fetch_ohlcv(symbol=symbol, timeframe=time_interval, since=start_time_since, limit=limit)
      
        df = pd.DataFrame(df, dtype=float)
        df_list.append(df)
   
        t = pd.to_datetime(df.iloc[-1][0], unit='ms')
        start_time_since = exchange.parse8601(str(t))
 
        if t >= end_time or df.shape[0] <= 1:
            break
    
        time.sleep(1)


    df = pd.concat(df_list, ignore_index=True)
    # print (df)
    # exit()
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                       3: 'low', 4: 'close', 5: 'volume'}, inplace=True)  # 重命名
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')  # 整理时间
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]  # 整理列的顺序


    df = df[df['candle_begin_time'].dt.date == pd.to_datetime(start_time).date()]

    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.sort_values('candle_begin_time', inplace=True)
    df.reset_index(drop=True, inplace=True)


    path = os.path.join(path, exchange.id)
    if os.path.exists(path) is False:
        os.mkdir(path)

    if type == 's':
        path = os.path.join(path, 'spot')
    else:
        path = os.path.join(path, 'future')
    if os.path.exists(path) is False:
        os.mkdir(path)

    path = os.path.join(path, str(pd.to_datetime(start_time).date()))
    if os.path.exists(path) is False:
        os.mkdir(path)
 
    file_name = '_'.join([symbol.replace('/', '-'), time_interval]) + '.csv'
    # print ('file_name',file_name)
    path = os.path.join(path, file_name)
    # print ('file_path',path)
    # save data
    df.to_csv(path, index=False)






import pandas as pd
import datetime
import ccxt
from library import configs

pd.options.display.max_columns = None

exchange = ccxt.binance()
# exchange = ccxt.binance({
#     'enableRateLimit': True,
#     'options': {'defaultType': 'future'}
# })

market = exchange.load_markets()
market = pd.DataFrame(market).T
symbol_list = ['BTC/USDT', 'ETH/USDT', 'EOS/USDT', 'LTC/USDT']
# symbol_list = ['1000SHIB/USDT', 'AAVE/USDT', 'UNI/USDT', 'DOGE/USDT', 'FIL/USDT']

path = 'data/history_candle_data/binance/spot'

begin_date = '2021-05-16'
end_date = '2021-06-04'
date_list = []
date = pd.to_datetime(begin_date)
while date <= pd.to_datetime(end_date):
    date_list.append(str(date))
    date += datetime.timedelta(days=1)

error_list = []

for date in date_list:
    for symbol in symbol_list:
        if not symbol.endswith('/USDT'):
            continue

        for time_interval in ['5m', '15m']:
            print('date: {}'.format(date), 'exchange: {}'.format(exchange.id), 'symbol:{}'.format(symbol),
                  'time_interval:{}'.format(time_interval))

            try:
                fetch.save_spot_candle_data_from_exchange(exchange, symbol, time_interval, date, path, 's')
            except Exception as e:
                print(e)
                error_list.append('_'.join([exchange.id, symbol, time_interval]))
