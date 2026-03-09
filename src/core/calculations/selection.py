import numpy as np
import pandas as pd

from src.core.config.constants import (
    DEFAULT_CORRELATION_THRESHOLD,
    DEFAULT_MIN_ALPHA,
    DEFAULT_MAX_NA_PCT,
    DEFAULT_MIN_IC,
    DEFAULT_N_FACTORS,
)


def calculate_correlation_matrix(results_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate correlation matrix between factors.

    Args:
        results_df: DataFrame with factor returns (Date, factor, ret)

    Returns:
        Correlation matrix DataFrame
    """
    pivot_df = results_df.pivot(index="Date", columns="factor", values="ret")
    corr_matrix = pivot_df.corr()

    return corr_matrix


def select_best_features(
    metrics_df: pd.DataFrame,
    correlation_matrix: pd.DataFrame,
    N: int = DEFAULT_N_FACTORS,
    correlation_threshold: float = DEFAULT_CORRELATION_THRESHOLD,
    a_min: float = DEFAULT_MIN_ALPHA,
    max_na_pct: float = DEFAULT_MAX_NA_PCT,
    min_ic: float = DEFAULT_MIN_IC,
    rank_by: str = "Alpha",
) -> tuple[list, dict[str, str]]:
    """
    Select N best features based on alpha or IC and low correlation.
    Also classifies all factors into categories.

    Args:
        metrics_df: DataFrame with feature metrics
        correlation_matrix: Correlation matrix of features
        N: Number of features to select
        correlation_threshold: Maximum allowed correlation
        a_min: Minimum absolute annualized alpha %
        max_na_pct: Maximum allowed NA percentage
        min_ic: Minimum absolute IC threshold
        rank_by: Metric to rank by ("Alpha" or "IC")

    Returns:
        Tuple of (selected feature names, classifications dict)
        Classifications: "best", "below_alpha", "correlation_conflict", "n_limit", "high_na", or "below_ic"
    """
    classifications = {}
    selected_features = []
    selected_indices = []

    corr_arr = correlation_matrix.values
    col_to_idx = {c: i for i, c in enumerate(correlation_matrix.columns)}

    sort_col = "IC" if rank_by == "IC" else "annualized alpha %"
    sorted_metrics = metrics_df.sort_values(by=sort_col, key=abs, ascending=False)

    has_na_col = "NA %" in sorted_metrics.columns
    has_ic_col = "IC" in sorted_metrics.columns

    columns = sorted_metrics["column"].to_numpy()
    alphas = sorted_metrics["annualized alpha %"].to_numpy()
    na_pcts = sorted_metrics["NA %"].to_numpy() if has_na_col else np.zeros(len(sorted_metrics))
    ics = sorted_metrics["IC"].to_numpy() if has_ic_col else None

    feat_indices = np.array([col_to_idx.get(c, -1) for c in columns])

    valid_na = na_pcts <= max_na_pct
    # skip alpha filter if a_min is effectively 0
    skip_alpha_filter = a_min < 1e-9
    valid_alpha = np.ones(len(alphas), dtype=bool) if skip_alpha_filter else np.abs(alphas) >= a_min
    valid_ic = np.abs(ics) >= min_ic if ics is not None else None

    for i in range(len(columns)):
        feature = columns[i]

        if not valid_na[i]:
            classifications[feature] = "high_na"
            continue

        if rank_by == "IC":
            if valid_ic is not None and not valid_ic[i]:
                classifications[feature] = "below_ic"
                continue
        else:
            if not valid_alpha[i]:
                classifications[feature] = "below_alpha"
                continue

        if len(selected_features) >= N:
            classifications[feature] = "n_limit"
            continue

        feat_idx = feat_indices[i]
        if feat_idx < 0 or (
            len(selected_indices) > 0
            and np.any(np.abs(corr_arr[feat_idx, selected_indices]) >= correlation_threshold)
        ):
            classifications[feature] = "correlation_conflict"
            continue

        selected_features.append(feature)
        selected_indices.append(feat_idx)
        classifications[feature] = "best"

    return selected_features, classifications
