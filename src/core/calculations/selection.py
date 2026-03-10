import numpy as np
import polars as pl

from src.core.config.constants import (
    DEFAULT_CORRELATION_THRESHOLD,
    DEFAULT_MIN_ALPHA,
    DEFAULT_MAX_NA_PCT,
    DEFAULT_MIN_IC,
    DEFAULT_N_FACTORS,
)


def calculate_correlation_matrix(results_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate correlation matrix between factors.

    Args:
        results_df: DataFrame with factor returns (Date, factor, ret)

    Returns:
        Correlation matrix DataFrame
    """
    pivot_df = results_df.pivot(index="Date", on="factor", values="ret")
    factor_df = pivot_df.select(pl.exclude("Date"))
    corr_df = factor_df.pearson_corr()
    return corr_df.insert_column(0, pl.Series("factor", factor_df.columns))


def select_best_features(
    metrics_df: pl.DataFrame,
    correlation_matrix: pl.DataFrame,
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


    factor_names = correlation_matrix["factor"].to_list()
    corr_arr = correlation_matrix.select(pl.exclude("factor")).to_numpy()
    col_to_idx = {c: i for i, c in enumerate(factor_names)}

    sort_col = "IC" if rank_by == "IC" else "annualized alpha %"
    sorted_metrics = metrics_df.sort(pl.col(sort_col).abs(), descending=True)

    has_na_col = "NA %" in sorted_metrics.columns
    has_ic_col = "IC" in sorted_metrics.columns

    columns = sorted_metrics["column"].to_numpy()
    alphas = sorted_metrics["annualized alpha %"].to_numpy()
    na_pcts = sorted_metrics["NA %"].to_numpy() if has_na_col else np.zeros(len(sorted_metrics))
    ics = sorted_metrics["IC"].to_numpy() if has_ic_col else None

    feat_indices = np.array([col_to_idx[c] for c in columns])

    valid_na = na_pcts <= max_na_pct

    if rank_by == "IC":
        valid_metric = np.abs(ics) >= min_ic if ics is not None else np.ones(len(columns), dtype=bool)
        metric_fail_label = "below_ic"
    else:
        valid_metric = np.abs(alphas) >= a_min if a_min >= 1e-9 else np.ones(len(alphas), dtype=bool)
        metric_fail_label = "below_alpha"

    for i, feature in enumerate(columns):
        if not valid_na[i]:
            classifications[feature] = "high_na"
        elif not valid_metric[i]:
            classifications[feature] = metric_fail_label
        elif len(selected_features) >= N:
            classifications[feature] = "n_limit"
        elif (
            selected_indices
            and np.any(np.abs(corr_arr[feat_indices[i], selected_indices]) >= correlation_threshold)
        ):
            classifications[feature] = "correlation_conflict"
        else:
            selected_features.append(feature)
            selected_indices.append(feat_indices[i])
            classifications[feature] = "best"

    return selected_features, classifications
