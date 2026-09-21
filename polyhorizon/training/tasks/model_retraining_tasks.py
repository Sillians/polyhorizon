import mlflow
from mlflow.tracking import MlflowClient
from prefect import task, get_run_logger
from typing import Any, Dict, Tuple

from polyhorizon.training.configs.settings import Config
from polyhorizon.training.src.monitoring.main_drift_detection import SymbolDriftDetector
from polyhorizon.training.src.data.generate_training_dataframe import PostgresDataLoader


@task(name="Drift Detection Check", retries=3, retry_delay_seconds=10)
def drift_detection_task(config: Config) -> Tuple[bool, Any, Dict[str, Any]]:
    loader = PostgresDataLoader(config)
    detector = SymbolDriftDetector(config)
    
    # 1. Fetch data
    ref_df, cur_df = loader.fetch_drift_data()
    
    # 2. Run analysis
    drift_payload = detector.run_full_suite(ref_df, cur_df)
    
    # 3. Decision Logic:
    # Retrain if more than X% of symbols drift
    symbol_results = drift_payload.get("symbol_results", {})
    drifted_symbols = [s for s, r in symbol_results.items() if r.get('drift_detected')]
    total_symbols = len(symbol_results)
    pct_drifted = len(drifted_symbols) / total_symbols if total_symbols else 0.0
    target_drift = drift_payload.get("target_drift", {})
    target_drift_detected = bool(target_drift.get("drift_detected"))
    needs_retraining = (pct_drifted >= config.drift.threshold) or target_drift_detected
    
    summary = {
        "total_symbols": total_symbols,
        "drifted_symbols": drifted_symbols,
        "pct_drifted": pct_drifted,
        "threshold": config.drift.threshold,
        "target_drift_detected": target_drift_detected,
    }
    
    return needs_retraining, cur_df, {"results": drift_payload, "summary": summary}


@task(name="Fetch Last Best Params")
def get_last_best_params(config: Config) -> Dict[str, Any]:
    client = MlflowClient()
    logger = get_run_logger()
    experiment = client.get_experiment_by_name(config.mlflow.experiment_name)
    
    if experiment is None:
        logger.warning("No MLflow experiment found. Falling back to config defaults.")
        return _default_model_params(config)
    
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="tags.optuna_study_name = 'tft_tuning'",
        run_view_type=mlflow.entities.ViewType.ACTIVE_ONLY,
        max_results=1,
        order_by=["attributes.start_time DESC"]
    )
    
    if not runs:
        logger.warning("No tuning runs found. Falling back to config defaults.")
        return _default_model_params(config)

    raw_params = runs[0].data.params

    # Cast strings back to their expected types
    typed_params = {}
    for k, v in raw_params.items():
        if isinstance(v, str) and v.isdigit():
            typed_params[k] = int(v)
        elif isinstance(v, str) and v.replace('.', '', 1).isdigit():
            typed_params[k] = float(v)
        else:
            typed_params[k] = v
    return typed_params


def _default_model_params(config: Config) -> Dict[str, Any]:
    return {
        "learning_rate": config.model.learning_rate,
        "hidden_size": config.model.hidden_size,
        "attention_head_size": config.model.attention_head_size,
        "dropout": config.model.dropout,
        "hidden_continuous_size": config.model.hidden_continuous_size,
    }
