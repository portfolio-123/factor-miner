import numpy as np
import polars as pl

from src.core.types.models import RANK_CONFIG, AnalysisParams


def calculate_correlation_matrix(lf: pl.LazyFrame, factor_columns: list[str]):
    ranked = (
        lf.select(["Date", *factor_columns])
        .with_columns([pl.col(f).rank(method="average").over("Date").cast(pl.Float32).alias(f) for f in factor_columns])
        .collect()
    )
    length = len(factor_columns)
    corr_sum = np.zeros((length, length), dtype=np.float64)
    corr_count = np.zeros((length, length), dtype=np.int32)
    for date_df in ranked.partition_by("Date", maintain_order=False):
        x = date_df.select(factor_columns).to_numpy()
        if x.shape[0] < 2:  # skip dates with less than 2 stocks
            continue
        # ignore nans and inf
        corr = np.ma.corrcoef(np.ma.masked_invalid(x), rowvar=False).filled(np.nan)
        valid = np.isfinite(corr)
        corr_sum[valid] += corr[valid]
        corr_count[valid] += 1

    # get the average (total sum / dates)
    avg_corr = np.divide(corr_sum, corr_count, out=np.full((length, length), np.nan, dtype=np.float64), where=corr_count > 0)
    np.fill_diagonal(avg_corr, 1.0)
    return pl.DataFrame({"factor": factor_columns, **{factor_columns[j]: avg_corr[:, j].tolist() for j in range(length)}})


def select_best_factors(metrics_df: pl.DataFrame, corr_matrix: pl.DataFrame, params: AnalysisParams) -> tuple[list[str], dict[str, str]]:
    # iterate all factors from strongest to weakest (based on rank_by metric). adds a classification to every factor, indicating why it was excluded or "best" if it passed all the filters

    # assign an index to each factor to later look up its correlation pairs as corr_arr[idx, idx_pair]
    corr_arr = corr_matrix.select(pl.exclude("factor")).to_numpy()
    col_to_idx = {c: i for i, c in enumerate(corr_matrix["factor"].to_list())}

    rank_config = RANK_CONFIG[params.rank_by]

    sort_by, is_desc = rank_config.get_sorting(params.high_quantile)

    # sort factors by rank_by metric, best first
    sorted_metrics = metrics_df.sort(sort_by, descending=is_desc)
    # array containing all factor names
    factors: list[str] = sorted_metrics["column"].to_list()
    factor_idx = np.array([col_to_idx[f] for f in factors])

    data = sorted_metrics[params.rank_by].to_numpy()
    valid_rank_by = (data <= params.min_rank_metric) if params.high_quantile == 0 else (data >= params.min_rank_metric)

    valid_na = sorted_metrics["na_pct"].to_numpy() <= params.max_na_pct

    classifications: dict[str, str] = {}
    selected_features: list[str] = []
    selected_indices: list[int] = []

    # go through each factor, check filters 1 by 1. if all filters pass, classify as best factor
    for i, feature in enumerate(factors):
        if not valid_na[i]:
            label = "high_na"
        elif not valid_rank_by[i]:
            label = f"below_rank_metric"
        elif len(selected_indices) >= (params.n_factors if params.n_factors is not None else float("inf")):
            label = "n_limit"
        elif selected_indices and np.any(np.abs(corr_arr[factor_idx[i], selected_indices]) >= params.correlation_threshold):
            label = "correlation_conflict"
        else:
            selected_features.append(feature)
            selected_indices.append(factor_idx[i])
            label = "best"

        classifications[feature] = label

    return selected_features, classifications
