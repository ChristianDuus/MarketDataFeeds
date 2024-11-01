JSON_KEYFILE_NAME = "credentials.json"

aggregation_enabled = True  # Set to True for aggregating order books across exchanges

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
        'enabled': False,  # Enable Kraken
        'pairs': ['XBT/USDT', 'ETH/USDT']  # Adjust pairs as needed
    },
    'coinbase': {  # Currently not available due to lack of credentials for Coinbase
        'enabled': False,  # Set True or False based on your need
        'pairs': ['BTC-USDT', 'ETH-USDT']  # Coinbase format uses dash between pairs
    }
}

SYMBOL_MAPPING = {
    "BINANCE": {"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT"},
    "OKX": {"BTC-USDT": "BTC-USDT", "ETH-USDT": "ETH-USDT"},
    "KRAKEN": {"XBTUSD": "BTC-USDT", "ETHUSD": "ETH-USDT"},
    "COINBASE": {"BTC-USD": "BTC-USDT", "ETH-USD": "ETH-USDT"}
    # Add additional mappings as necessary
}

def normalize_pair(exchange, symbol):
    return SYMBOL_MAPPING.get(exchange, {}).get(symbol, symbol)