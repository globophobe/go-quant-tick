def normalize_symbol(feed: str, symbol: str) -> str:
    for char in ("-", "/", "_"):
        symbol = symbol.replace(char, "")
    return symbol
