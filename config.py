JSON_KEYFILE_NAME = "credentials.json"

exchanges = {
    'binance': {
        'enabled': True,
        'pairs': ['btcusdt', 'ethusdt']  # Binance have lower case names, no dash or slash
    },
    'okx': {
        'enabled': True,  # Set True or False based on your need
        'pairs': ['BTC-USDT', 'ETH-USDT']  # OKX format uses dash between pairs
    },
    'coinbase': {  # Currently not available due to lack of credentials for Coinbase
        'enabled': False,  # Set True or False based on your need
        'pairs': ['BTC-USDT', 'ETH-USDT']  # Coinbase format uses dash between pairs
    }
}
