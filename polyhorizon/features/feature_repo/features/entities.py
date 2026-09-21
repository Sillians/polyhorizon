from feast import Entity, ValueType

class Entities:
    """Defines reusable entities across multiple feature views."""    
    symbol = Entity(
        name="symbol",
        join_keys=["symbol"],
        value_type=ValueType.STRING,
        description="Stock ticker symbol",
    )