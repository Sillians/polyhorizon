"""Timestamp-based holdout boundaries and decoder-target leakage checks."""
import numpy as np
import pandas as pd


def timestamp_split(frame, group_ids, validation_ratio, encoder_length, prediction_length):
    if not 0 < validation_ratio < 1:
        raise ValueError("validation_ratio must be strictly between 0 and 1")
    if encoder_length < 2 or prediction_length < 1:
        raise ValueError("Split requires encoder_length >= 2 and prediction_length >= 1")
    frame = frame.copy()
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True, errors="raise")
    if frame.empty or frame[[*group_ids, "event_timestamp"]].isna().any().any():
        raise ValueError("Temporal split requires nonempty groups and valid timestamps")
    if frame.duplicated([*group_ids, "event_timestamp"]).any():
        raise ValueError("Duplicate group/timestamp rows make the split ambiguous")
    frame = frame.sort_values([*group_ids, "event_timestamp"]).reset_index(drop=True)
    # One step represents one observed bar, not elapsed wall time. Rebuild after
    # cleaning so missing features cannot create synthetic decoder target rows.
    frame["time_idx"] = frame.groupby(group_ids, observed=True).cumcount()
    timestamps = frame.event_timestamp.drop_duplicates().sort_values()
    train_count = int(np.floor(len(timestamps) * (1 - validation_ratio)))
    if train_count < 1 or train_count >= len(timestamps):
        raise ValueError("Not enough distinct timestamps for both split partitions")
    cutoff = timestamps.iloc[train_count - 1]
    train = frame.loc[frame.event_timestamp <= cutoff].copy()
    holdout = frame.loc[frame.event_timestamp > cutoff]
    expected = frame[group_ids].drop_duplicates()
    counts = expected.merge(train.groupby(group_ids, observed=True).size().rename("train_rows").reset_index(), on=group_ids, how="left")
    counts = counts.merge(holdout.groupby(group_ids, observed=True).size().rename("validation_rows").reset_index(), on=group_ids, how="left")
    counts[["train_rows", "validation_rows"]] = counts[["train_rows", "validation_rows"]].fillna(0)
    if ((counts.train_rows < encoder_length) | (counts.validation_rows < prediction_length)).any():
        raise ValueError("Every group needs a full encoder before the cutoff and a full prediction horizon after it")
    context = train.groupby(group_ids, observed=True).tail(encoder_length)
    validation = pd.concat([context, holdout]).sort_values([*group_ids, "time_idx"]).reset_index(drop=True)
    boundaries = train.groupby(group_ids, observed=True).time_idx.max().rename("training_last_idx").reset_index()
    return train, validation, cutoff, boundaries


def validation_window_mask(index, boundaries, group_ids):
    aligned = index.merge(boundaries, on=group_ids, how="left", validate="many_to_one", sort=False)
    return (aligned.time_idx_first_prediction > aligned.training_last_idx).to_numpy()


def assert_validation_targets_after_cutoff(index, frame, group_ids, cutoff):
    """Check every decoder step, not merely the validation frame's last date."""
    if index.empty:
        raise ValueError("Validation dataset contains no decoder windows")
    observed_groups = index[group_ids].drop_duplicates()
    expected_groups = frame[group_ids].drop_duplicates()
    if len(expected_groups.merge(observed_groups, on=group_ids, how="inner")) != len(expected_groups):
        raise ValueError("Validation decoder windows are missing one or more groups")
    # Join each window to its own group's actual bar timestamps. Dense indices
    # from timestamp_split mean no unobserved/gap-filled targets are accepted.
    windows = index[[*group_ids, "time_idx_first_prediction", "time_idx_last"]].copy()
    if (windows.time_idx_first_prediction > windows.time_idx_last).any():
        raise ValueError("Invalid validation decoder bounds")
    windows["time_idx"] = [np.arange(start, end + 1) for start, end in
                           zip(windows.time_idx_first_prediction, windows.time_idx_last)]
    targets = windows.explode("time_idx").merge(
        frame[[*group_ids, "time_idx", "event_timestamp"]],
        on=[*group_ids, "time_idx"], how="left", validate="many_to_one",
    )
    if targets.event_timestamp.isna().any() or not (targets.event_timestamp > cutoff).all():
        raise ValueError("Every validation target must have an observed timestamp strictly after the training cutoff")
    return targets.event_timestamp.min()
