"""Release-controlled product universe shared by all services."""

PRODUCT_SYMBOLS = ("NVDA", "AAPL", "MSFT")
DEFAULT_SYMBOL = PRODUCT_SYMBOLS[0]


def require_product_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if normalized not in PRODUCT_SYMBOLS:
        raise ValueError(f"Unsupported product symbol: {normalized}")
    return normalized


def ingestion_symbols(available, total_limit: int, connection_limit: int) -> list[str]:
    missing = set(PRODUCT_SYMBOLS) - set(available)
    if missing:
        raise ValueError(f"Governed universe is missing product symbols: {sorted(missing)}")
    if min(total_limit, connection_limit) < len(PRODUCT_SYMBOLS):
        raise ValueError("Ingestion limits must cover the entire product symbol allowlist")
    return list(PRODUCT_SYMBOLS)


def filter_product_frame(frame):
    """Exclude historical out-of-product rows from training and drift inputs."""
    return frame.loc[frame["symbol"].isin(PRODUCT_SYMBOLS)].copy()
