def normalize_symbol(feed: str, symbol: str) -> str:
    """Normalize symbol."""
    for char in ("-", "/", "_"):
        symbol = symbol.replace(char, "")
    return symbol
