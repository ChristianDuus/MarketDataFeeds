# config.py

# For Binance
#BINANCE_PAIRS = ["ethusdt"]
#BINANCE_PAIRS = ["btcusdt", "ethusdt"]

#SHEET_NAME = "Your Google Sheet Name"
JSON_KEYFILE_NAME = "credentials.json"


# config.py

exchanges = {
    'binance': {
        'enabled': True,
        'pairs': ['btcusdt', 'ethusdt'] # Binance have lower case names, no dash or slash
    },
    'okx': {
        'enabled': True,  # Set True or False based on your need
        'pairs': ['BTC-USDT', 'ETH-USDT']  # OKX format uses dash between pairs
    }
}


