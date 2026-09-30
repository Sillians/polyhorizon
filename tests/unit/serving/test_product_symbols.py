import pandas as pd
import pytest

from polyhorizon.core.product_symbols import (
    PRODUCT_SYMBOLS, filter_product_frame, ingestion_symbols, require_product_symbol,
)
from polyhorizon.serving.api.schemas.forecast import ForecastRequest
from polyhorizon.serving.configs.settings import ClientMetadataConfig


def test_ingestion_uses_product_order_not_snapshot_order():
    assert ingestion_symbols(["ZZZ", "MSFT", "AAPL", "NVDA"], 50, 50) == list(PRODUCT_SYMBOLS)


@pytest.mark.parametrize("available,total,connection", [
    (["NVDA"], 50, 50), (PRODUCT_SYMBOLS, 2, 50), (PRODUCT_SYMBOLS, 50, 2),
])
def test_ingestion_cannot_silently_drop_product_symbols(available, total, connection):
    with pytest.raises(ValueError):
        ingestion_symbols(available, total, connection)


def test_training_excludes_nonproduct_rows():
    frame = pd.DataFrame({"symbol": ["NVDA", "ZZZ", "AAPL"], "close": [1, 2, 3]})
    assert filter_product_frame(frame)["symbol"].tolist() == ["NVDA", "AAPL"]
    assert len(frame) == 3


def test_request_and_metadata_share_allowlist():
    assert ClientMetadataConfig().supported_symbols == list(PRODUCT_SYMBOLS)
    assert ForecastRequest(symbol=" nvda ").symbol == "NVDA"
    with pytest.raises(ValueError):
        ForecastRequest(symbol="ZZZ")
    with pytest.raises(ValueError):
        require_product_symbol("ZZZ")
    with pytest.raises(ValueError):
        ClientMetadataConfig(supported_symbols=["ZZZ"])
