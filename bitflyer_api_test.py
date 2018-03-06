import pybitflyer
import json

# bitFlyer API setting
public_api = pybitflyer.API()
bitFlyer_keys = json.load(open('bitFlyer_keys.json', 'r'))
api = pybitflyer.API(api_key=bitFlyer_keys['key'], api_secret=bitFlyer_keys['secret'])

# エントリー
callback = api.sendchildorder(product_code='FX_BTC_JPY', child_order_type='LIMIT', side='BUY', price=700000, size=0.001)
print(callback)