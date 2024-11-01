def format_order_data(exchange, data):
    # Default bids and asks to empty lists if they are not provided
    standardized_data = {
        "bids": data.get("bids", []),
        "asks": data.get("asks", []),
        "timestamp": data.get("timestamp"),
        "exchange": exchange
    }
    return standardized_data