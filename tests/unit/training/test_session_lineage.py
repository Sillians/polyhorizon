from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from polyhorizon.training.src.data.generate_training_dataframe import PostgresDataLoader
from polyhorizon.training.src.governance.champs_challenger_model_judge import ModelJudge
from polyhorizon.training.src.training.trainer import TFTTrainer


def test_training_query_requires_completed_qualified_release():
    config = SimpleNamespace(connection_parameters=None, data=SimpleNamespace(
        db_schema="feast", snapshot_table_name="snapshot", min_days_required=180))
    loader = PostgresDataLoader(config)
    query = loader.build_query()
    assert "publication.status = 'complete'" in query
    assert "nyse-full-session-v1" in query
    assert "snapshot.dataset_version = publication.dataset_id" in query
    loader.get_engine = Mock()
    with patch("pandas.read_sql", return_value=pd.DataFrame(columns=["symbol"])), pytest.raises(ValueError, match="qualified"):
        loader.fetch_data(track_mlflow=False)


def test_trainer_requires_qualified_parent_before_starting_run():
    trainer = TFTTrainer.__new__(TFTTrainer)
    with patch("mlflow.active_run", return_value=None), patch("mlflow.start_run") as start:
        with pytest.raises(ValueError, match="qualified"):
            trainer.train()
        start.assert_not_called()


def test_governance_cannot_approve_model_without_session_lineage():
    judge = ModelJudge.__new__(ModelJudge)
    judge.client = Mock()
    judge.model_name = "test"
    judge.client.get_model_version.return_value = SimpleNamespace(run_id="old-run")
    judge.client.get_run.return_value = SimpleNamespace(data=SimpleNamespace(tags={}))
    with pytest.raises(ValueError, match="qualified"):
        judge._promote("1", "first champion")
    judge.client.set_model_version_tag.assert_not_called()
    judge.client.set_registered_model_alias.assert_not_called()
