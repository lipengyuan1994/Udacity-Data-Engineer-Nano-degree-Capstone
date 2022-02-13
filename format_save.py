"""
type: future or spot; this task will convert the csv to h5 format and save it in corresponding folder
symble:
"""

import pandas as pd
import glob
from library import configs

type = 'spot'


pd.set_option('expand_frame_repr', False)  # display all columns

# symbol_list = ['FIL-USDT_15m','UNI-USDT_15m', 'DOGE-USDT_15m', 'AAVE-USDT_15m','1000SHIB-USDT_15m','XRP-USDT_15m']
symbol_list = ['EOS-USDT_15m', 'ETH-USDT_15m', 'BTC-USDT_15m']
for symbol in symbol_list:

    '''input path'''
    # path = configs.get_config('data_path', 'binance')['spot']
    path = './data/history_candle_data/binance/spot'
#     path = configs.get_config('data_path', 'binance')[type]
    path_list = glob.glob(path + "/*/*.csv")  #
    '''output path'''
#     h5path = configs.get_config('data_path', 'h5')[type]
    h5path = './data/h5/spot'
    save_path = h5path + '{}.h5'.format(symbol)

    # filter file on symbol
    path_list = list(filter(lambda x: symbol in x, path_list))

    df_list = []
    for path in sorted(path_list):
        print(path)
        df = pd.read_csv(path, encoding="GBK", parse_dates=['candle_begin_time'])
        df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]
        df_list.append(df)
        print(df.head(1))
    # print(    df_list )


    data = pd.concat(df_list, ignore_index=True)
    data.sort_values(by='candle_begin_time', inplace=False)
    data.reset_index(drop=False, inplace=False)

    '''save data'''
    data.to_hdf(save_path, key='df', mode='w')
    print(save_path)
    print(data.info())
    print(data.describe())
