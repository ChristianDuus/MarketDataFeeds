JSON_KEYFILE_NAME = "credentials.json"

aggregation_enabled = False  # Set to True for aggregating order books across exchanges

# Exchange configuration
exchanges = {
    'binance': {
        'enabled': True,
        'pairs': ['btcusdt', 'ethusdt']  # Binance have lower case names, no dash or slash
    },
    'okx': {
        'enabled': True,  # Set True or False based on your need
        'pairs': ['BTC-USDT', 'ETH-USDT']  # OKX format uses dash between pairs
    },
    'kraken': {
        'enabled': True,  # Enable Kraken
        'pairs': ['XBT/USDT', 'ETH/USDT']  # Adjust pairs as needed
    },
    'coinbase': {  # Currently not available due to lack of credentials for Coinbase
        'enabled': False,  # Set True or False based on your need
        'pairs': ['BTC-USDT', 'ETH-USDT']  # Coinbase format uses dash between pairs
    }
}
