import great_expectations as gx

SNAPSHOT_EXPECTATION_SUITE_VERSION = "v1"


def snapshot_table_expectations():
    """
    Registry of expectations for snapshot table.
    """
    return [
        # Column existence checks
        gx.expectations.ExpectColumnToExist(column="symbol"),
        gx.expectations.ExpectColumnToExist(column="event_timestamp"),
        gx.expectations.ExpectColumnToExist(column="close"),
        gx.expectations.ExpectColumnToExist(column="open"),
        gx.expectations.ExpectColumnToExist(column="total_volume"),
        gx.expectations.ExpectColumnToExist(column="rolling_avg_close"),
        gx.expectations.ExpectColumnToExist(column="rolling_volatility_close"),

        # Not null checks
        gx.expectations.ExpectColumnValuesToNotBeNull(column="symbol"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="event_timestamp"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="close"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="open"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="rolling_avg_close"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="rolling_volatility_close"),
        
        # Type checks
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="event_timestamp",
            type_='timestamp with time zone',
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="symbol",
            type_='text',
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="open",
            type_="double precision",
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="close",
            type_="double precision",
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="total_volume",
            type_="bigint",  
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="rolling_avg_close",
            type_="double precision",  
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="rolling_volatility_close",
            type_="double precision",  
        ),

    ]
