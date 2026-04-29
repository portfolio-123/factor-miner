import numpy as np
import polars as pl

from src.core.types.models import RANK_CONFIG, AnalysisParams


def calculate_correlation_matrix(lf: pl.LazyFrame, factor_columns: list[str]):
    length = len(factor_columns)
    corr_sum = np.zeros((length, length), dtype=np.float64)
    corr_count = np.zeros((length, length), dtype=np.int32)

    dates = lf.select(pl.col("Date").rle().struct.field("value").alias("Date")).collect().get_column("Date").to_list()

    ranks = [pl.col(c).fill_nan(None).rank(method="average").alias(c) for c in factor_columns]

    for date in dates:
        df = lf.filter(pl.col("Date") == date).select(ranks).collect()

        if df.height < 2:
            continue
        # ignore nans and inf
        corr = np.ma.corrcoef(np.ma.masked_invalid(df.to_numpy()), rowvar=False).filled(np.nan)

        valid = np.isfinite(corr)
        corr_sum[valid] += corr[valid]
        corr_count[valid] += 1

    # get the average (total sum / dates)
    avg_corr = np.divide(corr_sum, corr_count, out=np.full((length, length), np.nan, dtype=np.float64), where=corr_count > 0)
    np.fill_diagonal(avg_corr, 1.0)
    return pl.DataFrame({"factor": factor_columns, **{factor_columns[j]: avg_corr[:, j].tolist() for j in range(length)}})


def select_best_factors(
    dataset_lf: pl.LazyFrame, metrics_df: pl.DataFrame, params: AnalysisParams
) -> tuple[list[str], dict[str, str], pl.DataFrame]:

    rank_config = RANK_CONFIG[params.rank_by]
    sort_by, is_desc = rank_config.get_sorting(params.high_quantile)

    processed_metrics = metrics_df.with_columns(
        status=pl.when(pl.col("na_pct") > params.max_na_pct)
        .then(pl.lit("high_na"))
        .when(
            (pl.col(params.rank_by) > params.min_rank_metric)
            if params.high_quantile == 0
            else (pl.col(params.rank_by) < params.min_rank_metric)
        )
        .then(pl.lit("below_rank_metric"))
    ).sort(sort_by, descending=is_desc)

    classifications = dict(zip(processed_metrics["column"], processed_metrics["status"]))

    candidate_df = processed_metrics.filter(pl.col("status").is_null())

    candidates = candidate_df["column"].to_list()
    if not candidates:
        return [], classifications, pl.DataFrame()

    if len(candidates) == 1:
        feature = candidates[0]

        classifications[feature] = "best"
        corr_matrix = pl.DataFrame({"factor": [feature], feature: [1.0]})
        return [feature], classifications, corr_matrix

    corr_matrix = calculate_correlation_matrix(dataset_lf, candidates)
    corr_arr = corr_matrix.select(pl.exclude("factor")).to_numpy()

    selected_features: list[str] = []
    selected_indices: list[int] = []

    for i, feature in enumerate(candidates):
        if len(selected_features) >= (params.n_factors if params.n_factors is not None else float("inf")):
            classifications[feature] = "n_limit"
        elif selected_indices and np.any(np.abs(corr_arr[i, selected_indices]) >= params.correlation_threshold):
            classifications[feature] = "correlation_conflict"
        else:
            selected_features.append(feature)
            selected_indices.append(i)
            classifications[feature] = "best"

    return selected_features, classifications, corr_matrix
