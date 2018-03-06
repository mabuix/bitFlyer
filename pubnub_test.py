from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pnconfiguration import PNReconnectionPolicy
from pubnub.pubnub import PubNub, SubscribeListener

config = PNConfiguration()
# bitFlyer realtime api subscribe key.
config.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
# 自動で再接続する方針
config.reconnect_policy = PNReconnectionPolicy.LINEAR
pubnub = PubNub(config)

my_listener = SubscribeListener()
pubnub.add_listener(my_listener)
 
pubnub.subscribe().channels(['lightning_executions_FX_BTC_JPY']).execute()
my_listener.wait_for_connect()
print('connected')

result = my_listener.wait_for_message_on('lightning_executions_FX_BTC_JPY')
# 約定情報を出力
print(result.message)

pubnub.unsubscribe().channels(['lightning_executions_FX_BTC_JPY']).execute()
my_listener.wait_for_disconnect()
print('unsubscribed')
