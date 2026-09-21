from polyhorizon.features.utils.logger import get_logger
logger = get_logger("Great Expectation Validator")

def validate_batch(batch, expectations):
    """
    Executes expectations against a GE Batch.
    """
    results = []

    for expectation in expectations:
        result = batch.validate(expectation)
        results.append(result)

        if not result.success:
            logger.error(
                f"Expectation failed: "
                f"{expectation.__class__.__name__} | "
                f"{expectation.configuration.kwargs}"
            )

    success = all(r.success for r in results)
    return success, results
