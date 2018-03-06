"""
直近20秒のBuy/Sellボリュームを見て、BuyVolが強ければロング、SellVolが強ければショート、Buy/Sell反転した時が利確、兼、損切り

bitFlyer API Documentation
https://lightning.bitflyer.jp/docs?lang=ja&_ga=2.196656554.781680446.1517361666-114839911.1517361666

PubNub Documentation
https://www.pubnub.com/docs/python/pubnub-python-sdk#subscribe-channel

参考サイト
http://r17u.hatenablog.com/entry/2017/12/06/003734
https://github.com/r17u/bitFlyerScalpingBot/blob/master/bitFlyerScalpingBot.py
"""

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pnconfiguration import PNReconnectionPolicy
from pubnub.pubnub import PubNub, SubscribeListener
import pandas as pd
from datetime import datetime, timezone, timedelta
import pybitflyer
import json


# bitFlyer API setting
public_api = pybitflyer.API()
bitFlyer_keys = json.load(open('bitFlyer_keys.json', 'r'))
api = pybitflyer.API(api_key=bitFlyer_keys['key'], api_secret=bitFlyer_keys['secret'])

# dataframe for executions
df_all = pd.DataFrame(index=['datetime'],
                    columns=['id', 
                            'side', 
                            'price', 
                            'size', 
                            'exec_date', 
                            'buy_child_order_acceptance_id', 
                            'sell_child_order_acceptance_id'])

# プログラム起動時のbitFlyerのポジション情報取得
bf_positions = pd.DataFrame(api.getpositions(product_code='FX_BTC_JPY'))
local_position = 'NONE'
local_position_price = 0
if not(bf_positions.empty):
    local_position = bf_positions.ix[[0], ['side']].values.flatten()
    local_position_price = int(bf_positions.ix[[0], ['price']].values.flatten())
sum_profit = 0

# calc buy and sell volume from lightning_executions_FX_BTC_JPY message
def store_executions(channel, message, store_time_sec):
    # メッセージデータ取得
    df_new = pd.DataFrame(message)
    # 約定時間を日本時間に修正
    df_new['exec_date'] = pd.to_datetime(df_new['exec_date']) + timedelta(hours=9)

    global df_all
    # 取得したメッセージを追加
    df_all = df_all.append(df_new)
    # 取得したメッセージ分含め、全てのインデックス値を約定時間の値に更新
    df_all.index = df_all['exec_date']

    # 最後の約定時間を現在時刻として取得
    date_now = df_all.index[len(df_all) - 1]
    # 現在時間から指定秒数分前までのmessageデータ取得
    df_all = df_all.ix[df_all.index >= (date_now - timedelta(seconds=store_time_sec))]

    # ロングポジションの取引のみに絞って、取引高を合計
    buy_vol = df_all[df_all.apply(lambda x: x['side'], axis=1) == 'BUY']['size'].sum(axis=0)
    # print("buy_vol: %s" % buy_vol)
    # ショート(以下同)
    sell_vol = df_all[df_all.apply(lambda x: x['side'], axis=1) == 'SELL']['size'].sum(axis=0)
    # print("sell_vol: %s" % sell_vol)
    # 直前の約定金額
    ex_price = int(df_all.ix[[len(df_all) - 1], ['price']].values.flatten())
    # print("ex_price: %s" % ex_price)

    return df_all, buy_vol, sell_vol, ex_price

# close buy or sell position
def close(side, order_size, ex_price):
    oposit_side = 'NONE'
    if side == 'BUY':
        oposit_side = 'SELL'
    elif side == 'SELL':
        oposit_side = 'BUY'

    bf_positions = pd.DataFrame(api.getpositions(product_code='FX_BTC_JPY'))
    if not(bf_positions.empty):
        bf_position = bf_positions.ix[[0], ['side']].values.flatten()
        bf_position_price = int(bf_positions.ix[[0], ['price']].values.flatten())
        if bf_position == side:
            print('[' + side + ' Close]')
            callback = api.sendchildorder(product_code='FX_BTC_JPY', child_order_type='MARKET', side=oposit_side, size=order_size)
            print(callback)
            if not(callback.get('status')):
                ordered_profit = 0
                if side == 'BUY':
                    ordered_profit = (ex_price - bf_position_price) * order_size
                elif side == 'SELL':
                    ordered_profit = -(ex_price - bf_position_price) * order_size
                print('Order Complete!', 'ex_price:', ex_price, 'pos_price:', bf_position_price, 'profit:', format(ordered_profit, '.2f'))
                return 'NONE', ordered_profit
    else:
        return side, 0

# entry buy or sell position
def entry(side, order_size):
    print('[' + side + ' Entry]')
    callback = api.sendchildorder(product_code='FX_BTC_JPY', child_order_type='MARKET', side=side, size=order_size)
    print(callback)
    if not(callback.get('status')):
        print('Order Complete!')
        return side
    else:
        return 'NONE'

def received_message_task(channel, message):
    global local_position
    global local_position_price
    global sum_profit

    # 判断に使うデータを保持する秒数
    store_time_sec = 20
    # 注文量
    order_size = 0.003
    # 新規エントリーを決定する買いと売りの取引高差分
    entry_triger = 0

    df, buy_vol, sell_vol, ex_price = store_executions(channel, message, store_time_sec)

    # 利益と利益率の計算
    order_profit = 0
    if local_position == 'BUY':
        order_profit = (ex_price - local_position_price) * order_size
    elif local_position == 'SELL':
        order_profit = -(ex_price - local_position_price) * order_size
    order_profit_rate = order_profit / (ex_price * order_size)

    # 取引高の反転を見て、ポジションを閉じる
    if (local_position == 'BUY') and (buy_vol < sell_vol):
        local_position, ordered_profit = close('BUY', order_size, ex_price)
        sum_profit = sum_profit + ordered_profit
    elif (local_position == 'SELL') and (buy_vol > sell_vol):
        local_position, ordered_profit = close('SELL', order_size, ex_price)
        sum_profit = sum_profit + ordered_profit
    
    # 新規エントリー
    if (local_position == 'NONE'):
        if ((buy_vol - sell_vol) > entry_triger):
            local_position = entry('BUY', order_size)
            if local_position == 'BUY':
                local_position_price = ex_price
        elif (-(buy_vol - sell_vol) > entry_triger):
            local_position = entry('SELL', order_size)
            if local_position == 'SELL':
                local_position_price = ex_price

    # summary
    print(df.index[len(df) - 1].strftime('%H:%M:%S'),
          'BUY/SELL',
          format(buy_vol, '.2f'),
          format(sell_vol, '.2f'),
          'PRICE',
          ex_price,
          local_position,
          format(order_profit, '.2f'),
          format(order_profit_rate, '.4f'),
          'SUM_PROFIT',
          format(sum_profit, '.2f'))

# pubnub設定インスタンス
config = PNConfiguration()
# bitFlyer realtime api subscribe key.
config.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
# 3秒おきに自動で再接続する方針 https://www.pubnub.com/docs/python/reconnection-policies
config.reconnect_policy = PNReconnectionPolicy.LINEAR
# pubnubインスタンス生成
pubnub = PubNub(config)

def main(channels):
    class BitflyerSubscriberCallback(SubscribeCallback):
        def presence(self, pubnub, presence):
            pass
        def status(self, pubnub, status):
            if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
                pass
            elif status.category == PNStatusCategory.PNConnectedCategory:
                pass
            elif status.category == PNStatusCategory.PNReconnectedCategory:
                pass
            elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
                pass
        def message(self, pubnub, message):
            """ bitFlyer → PubNub から送信されるメッセージに対して処理を実行 """
            try:
                received_message_task(message.channel, message.message)

                # ストリーミング中のメッセージの出力
                # print("MESSAGE: %s" % message.message)
            except:
                print('Could not do received_message_task.')

    listener = BitflyerSubscriberCallback()
    pubnub.add_listener(listener)
    pubnub.subscribe().channels(channels).execute()

if __name__ == '__main__':
    # 引数に約定情報のチャンネル名を指定
    main(['lightning_executions_FX_BTC_JPY'])
