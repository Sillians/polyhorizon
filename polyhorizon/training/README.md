# PolyHorizon Training and Model Governance

## Qualification required for every champion

The first champion must qualify before assignment, even when there is no incumbent.
Each evaluation window must have at least `minimum_evaluation_samples` forecast
sequences (default 100; horizon cells are not counted as independent samples),
finite metrics, and an MAE improvement of at least
`minimum_baseline_mae_improvement` (default 0.02) over predicting zero log return
on the same held-out targets. A zero-error baseline cannot be beaten and fails
qualification. Overlapping sequences can be correlated; this count is not an
effective independent sample-size estimate.

All predictions and targets must be finite and aligned, with no crossing quantiles.
For every quantile and horizon, the absolute difference between empirical
`P(target <= predicted_quantile)` and the quantile level must be at most
`maximum_calibration_error` (default 0.10). Configured horizon and stability gates
also apply to the first champion. Every window must pass; missing or nonfinite
results cannot be hidden by averaging. Thresholds live under `governance` in the
training YAML and should be tuned using representative held-out data.

Governance marks promoted versions `governance_qualification=passed-v1`. Serving
bootstrap can only select READY versions carrying this qualification, never an
arbitrary new registered version. Existing aliases are left unchanged; re-evaluate
legacy models through governance to qualify them for future bootstrap.

Training and drift inputs use the shared [product allowlist](../../docs/PRODUCT_SYMBOLS.md).
Snapshot reads and preprocessing exclude symbols outside NVDA, AAPL, and MSFT.

The training service creates Temporal Fusion Transformer models from Feast
offline snapshots, records complete experiment lineage in MLflow, and controls
promotion through a champion/challenger workflow.

## Model Lifecycle

1. Load the offline feature snapshot from Postgres.
2. Apply the same time, market-calendar, log, and return transformations expected
   by serving.
3. Build leakage-safe training and validation `TimeSeriesDataSet` objects.
4. Optionally tune model hyperparameters.
5. Train the final quantile TFT and log validation metrics.
6. Persist fitted dataset parameters with the model artifact.
7. Register the model version and assign `@challenger`.
8. Compare challenger and champion on the validation window.
9. Move `@champion` only when governance thresholds pass. If no champion exists,
   the first model passing absolute qualification becomes the initial champion.

## Temporal validation boundary

After removing unusable feature rows, dataset construction sorts distinct UTC
`event_timestamp` values and assigns the first
`floor(timestamp_count * (1 - validation_ratio))` timestamps to training.
The last of those timestamps is one shared cutoff across all symbols: training
rows are at or before it, and every validation target must be strictly after it.
Equal timestamps cannot straddle the split, even with unequal symbol histories.

Validation retains up to `max_encoder_length` pre-cutoff rows per symbol only
as encoder context. Generated TFT windows whose decoder starts at or before the
boundary are removed. A second check resolves **every decoder target step** back
to its observed timestamp and rejects any missing, equal-to-cutoff, or earlier
target. Validation reuses encoders and normalization fitted only on training.

Bar indices are rebuilt per symbol after cleaning and timestamp sorting; they
represent observed bars, not elapsed wall time. Duplicate group/timestamp rows,
invalid timestamps, invalid validation ratios, and symbols lacking a full
encoder before or full prediction horizon after the cutoff fail closed.
MLflow's `temporal_split.json` records the UTC cutoff, earliest validation target,
row/window counts, encoder-context count, ratio, and split-method version.

Regression tests include unequal/missing symbol histories and inspection of
actual TFT dataloader decoder timestamps:

```bash
uv run pytest -q tests/unit/training/test_temporal_split.py
```

## Serving Contract

The model artifact includes `dataset_parameters` from the fitted training
dataset. These contain categorical encoders, target normalization, feature
roles, and encoder/decoder lengths. The serving service uses them through
`TimeSeriesDataSet.from_parameters(...)` to prevent train/serve preprocessing
drift.

Both model logging paths also serialize a versioned `target_contract` on the
model and include it in MLflow model metadata. It records the implemented
`log(close_t / close_t_minus_1)` target, per-symbol one-bar steps, and the `log`
price conversion method. Export rejects a dataset whose target column differs
from the implemented `target` column. Serving validates this artifact contract
at startup and reload; registry tags alone are not authoritative.

Registered versions also receive forecast-semantics tags:

- `forecast_horizon`
- `target_definition`
- `target_column`

## Alias Ownership

- `@challenger`: assigned to every newly registered model by training.
- `@champion`: assigned by governance after evaluation.
- Initial serving bootstrap: serving may assign a missing `@champion` to the
  newest governance-qualified READY version, but it never moves an existing champion.

## Governance Gates

Promotion considers:

- Directional hit rate.
- Mean absolute error.
- Composite score improvement.
- Maximum permitted MAE degradation.
- Optional stability and per-horizon gates.

Thresholds are configured in `configs/training_config.yaml` under `governance`.

## Entry Points

- Training flow: `polyhorizon.training.flow.model_training_flow:model_training_flow`
- Retraining flow: `polyhorizon.training.flow.model_retraining_flow:model_retraining_flow`
- Prefect tasks: `polyhorizon/training/tasks`

Run locally:

```bash
uv sync --extra training
uv run python -m polyhorizon.training.flow.model_training_flow
```

Deploy through Prefect:

```bash
make training-build-deploy
```

## Configuration

Primary configuration: `polyhorizon/training/configs/training_config.yaml`

Important sections:

- `features`: TFT static, known, and unknown feature roles.
- `training`: encoder length, prediction length, quantiles, and optimization.
- `mlflow`: tracking endpoint and experiment metadata.
- `model_registry`: model name, artifact path, and aliases.
- `governance`: promotion and risk thresholds.
- `monitoring`: Pushgateway metric configuration.

The encoder length, prediction length, feature roles, target semantics, and
quantiles must remain compatible with serving configuration.

## Operational Notes

- MLflow, Postgres, and object storage must be reachable before training starts.
- The feature snapshot must contain enough history per symbol for the encoder and
  validation windows.
- A newly registered challenger is not production traffic until governance moves
  `@champion` and serving reloads the alias or restarts.
