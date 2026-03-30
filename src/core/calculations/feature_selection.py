import numpy as np
import polars as pl

from src.core.types.models import AnalysisParams


def calculate_correlation_matrix(factor_returns_wide: pl.DataFrame) -> pl.DataFrame:
    df = factor_returns_wide.select(pl.exclude("factor")).drop_nulls()
    return df.corr().insert_column(0, pl.Series("factor", df.columns))


def select_best_factors(
    metrics_df: pl.DataFrame,
    corr_matrix: pl.DataFrame,
    params: AnalysisParams,
) -> tuple[list[str], dict[str, str]]:
    # iterate all factors from strongest to weakest (based on rank_by metric). adds a classification to every factor, indicating why it was excluded or "best" if it passed all the filters

    # assign an index to each factor to later look up its correlation pairs as corr_arr[idx, idx_pair]
    corr_arr = corr_matrix.select(pl.exclude("factor")).to_numpy()
    col_to_idx = {c: i for i, c in enumerate(corr_matrix["factor"].to_list())}

    # sort factors by rank_by metric, best first
    sorted_metrics = metrics_df.sort(pl.col(params.rank_by).abs(), descending=True)
    # array containing all factor names
    factors = sorted_metrics["column"].to_list()
    factor_idx = np.array([col_to_idx[f] for f in factors])

    valid_rank_by = np.abs(sorted_metrics[params.rank_by].to_numpy()) >= getattr(
        params, f"min_{params.rank_by}"
    )
    valid_na = sorted_metrics["na_pct"].to_numpy() <= params.max_na_pct

    classifications = {}
    selected_features = []
    selected_indices = []

    # go through each factor, check filters 1 by 1. if all filters pass, classify as best factor
    for i, feature in enumerate(factors):
        if not valid_na[i]:
            label = "high_na"
        elif not valid_rank_by[i]:
            label = f"below_{params.rank_by}"
        elif len(selected_indices) >= params.n_factors:
            label = "n_limit"
        elif selected_indices and np.any(
            np.abs(corr_arr[factor_idx[i], selected_indices])
            >= params.correlation_threshold
        ):
            label = "correlation_conflict"
        else:
            selected_features.append(feature)
            selected_indices.append(factor_idx[i])
            label = "best"

        classifications[feature] = label

    return selected_features, classifications
