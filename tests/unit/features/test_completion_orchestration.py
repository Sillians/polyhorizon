from unittest.mock import patch

import pytest

from polyhorizon.features.flow.feature_store_flow import feature_store_flow
from polyhorizon.streaming.tasks.spark_streaming_flow import spark_streaming_flow


def test_feature_flow_requires_completion_manifest():
    with patch("polyhorizon.features.flow.feature_store_flow.get_run_logger"), patch(
        "polyhorizon.features.flow.feature_store_flow.publish_feature_release"
    ) as publish:
        with pytest.raises(ValueError, match="dataset_release"):
            feature_store_flow.fn()
        publish.assert_not_called()


def test_feature_flow_passes_exact_manifest():
    manifest = {"test": "pinned manifest"}
    with patch("polyhorizon.features.flow.feature_store_flow.get_run_logger"), patch(
        "polyhorizon.features.flow.feature_store_flow.publish_feature_release", return_value={"status": "success"}
    ) as publish:
        assert feature_store_flow.fn(dataset_release=manifest, run_observability=False) == {"status": "success"}
        publish.assert_called_once_with(manifest, True)


@pytest.mark.parametrize("failure", [True, False])
def test_streaming_only_dispatches_after_successful_completion(failure):
    with patch("polyhorizon.streaming.tasks.spark_streaming_flow.market_hours_check", return_value=True), patch(
        "polyhorizon.streaming.tasks.spark_streaming_flow.is_streaming_active", return_value=False
    ), patch("polyhorizon.streaming.tasks.spark_streaming_flow.streaming_health_check"), patch(
        "polyhorizon.streaming.tasks.spark_streaming_flow.submit_spark_job"
    ) as submit, patch("polyhorizon.streaming.tasks.spark_streaming_flow.publish_completed_dataset") as publish:
        submit.return_value = {"dataset_id": "completed"}
        if failure:
            submit.side_effect = RuntimeError("Gold failed")
            with pytest.raises(RuntimeError, match="Gold failed"):
                spark_streaming_flow.fn()
            publish.assert_not_called()
        else:
            spark_streaming_flow.fn(qualification_only=False)
            publish.assert_called_once_with(submit.return_value)


def test_qualification_mode_never_publishes():
    with patch("polyhorizon.streaming.tasks.spark_streaming_flow.market_hours_check", return_value=True), patch(
        "polyhorizon.streaming.tasks.spark_streaming_flow.is_streaming_active", return_value=False
    ), patch("polyhorizon.streaming.tasks.spark_streaming_flow.streaming_health_check"), patch(
        "polyhorizon.streaming.tasks.spark_streaming_flow.submit_spark_job", return_value={"dataset_id": "qualified"}
    ), patch("polyhorizon.streaming.tasks.spark_streaming_flow.publish_completed_dataset") as publish:
        assert spark_streaming_flow.fn()["published"] is False
        publish.assert_not_called()
