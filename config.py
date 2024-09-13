JSON_KEYFILE_NAME = "credentials.json"

exchanges = {
    'binance': {
        'enabled': False,
        'pairs': ['btcusdt', 'ethusdt']  # Binance have lower case names, no dash or slash
    },
    'okx': {
        'enabled': False,  # Set True or False based on your need
        'pairs': ['BTC-USDT', 'ETH-USDT']  # OKX format uses dash between pairs
    },
    'kraken': {
        'enabled': True,  # Enable Kraken
        'pairs': ['XBT/USD', 'ETH/USD']  # Adjust pairs as needed
    },
    'coinbase': {  # Currently not available due to lack of credentials for Coinbase
        'enabled': False,  # Set True or False based on your need
        'pairs': ['BTC-USDT', 'ETH-USDT']  # Coinbase format uses dash between pairs
    }
}
