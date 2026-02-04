import pandas as pd
import numpy as np
import scipy.stats as stats
from typing import Callable, List, Optional
from src.services.readers import ParquetDataReader


def calculate_benchmark_returns(
    raw_data: pd.DataFrame,
    benchmark_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calculate benchmark returns for each date in the dataset based on data frequency.

    Args:
        raw_data: Dataset with 'Date' column
        benchmark_data: Benchmark price data with 'dt' and 'close' columns

    Returns:
        DataFrame with 'benchmark' column added
    """
    df = raw_data.copy()
    benchmark_df = benchmark_data.copy()

    benchmark_df["dt"] = pd.to_datetime(benchmark_df["dt"])

    benchmark_df = benchmark_df.sort_values("dt").dropna(subset=["dt", "close"])

    unique_dates = pd.DataFrame({"Date": df["Date"].unique()}).sort_values("Date")
    unique_dates["Prev_Date"] = unique_dates["Date"] - pd.Timedelta(days=7)

    current_prices = pd.merge_asof(
        unique_dates,
        benchmark_df[["dt", "close"]],
        left_on="Date",
        right_on="dt",
        direction="backward",
        tolerance=pd.Timedelta(days=4),
    ).rename(columns={"close": "curr_price"})

    last_period_prices = pd.merge_asof(
        current_prices,
        benchmark_df[["dt", "close"]],
        left_on="Prev_Date",
        right_on="dt",
        direction="backward",
        tolerance=pd.Timedelta(days=4),
    ).rename(columns={"close": "prev_price"})

    last_period_prices["benchmark"] = (
        last_period_prices["curr_price"] - last_period_prices["prev_price"]
    ) / last_period_prices["prev_price"]

    result_df = df.merge(
        last_period_prices[["Date", "benchmark"]], on="Date", how="left"
    )
    return result_df


def calculate_future_performance(
    raw_data: pd.DataFrame,
    price_column: str,
) -> pd.DataFrame:
    """
    Add a column "future performance" to the dataframe, which is the return of the next period for that same stock.
    (e.g. 0.07, or 7%)

    Args:
        raw_data: DataFrame with Date, Ticker, price columns
        price_column: Name of the price column

    Returns:
        DataFrame with Date, Ticker, and 'Future Perf' columns
    """
    df = raw_data[["Date", "Ticker", price_column]].copy()

    # sort by Ticker and Date
    df = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    # shift to get next period's values for each ticker
    df["Next_Date"] = df.groupby("Ticker")["Date"].shift(-1)
    df["Next_Price"] = df.groupby("Ticker")[price_column].shift(-1)

    # calculate return only where there is a price and next price
    valid_mask = (df[price_column].notna()) & (df["Next_Price"].notna())

    # only add future perf column for valid rows
    df["Future Perf"] = float("nan")
    df.loc[valid_mask, "Future Perf"] = (
        df.loc[valid_mask, "Next_Price"] - df.loc[valid_mask, price_column]
    ) / df.loc[valid_mask, price_column]

    # drop temporary columns
    df = df.drop(columns=["Next_Date", "Next_Price", price_column])

    return df


def analyze_factors(
    future_perf_df: pd.DataFrame,
    reader: ParquetDataReader,
    *,
    factor_columns: Optional[List[str]] = None,
    top_pct: float = 30.0,
    bottom_pct: float = 30.0,
    progress_fn: Optional[Callable[[int, int, str], None]] = None,
    batch_size: int = 50,
) -> pd.DataFrame:
    """
    Analyze factors by calculating top X% vs bottom X% performance difference.
    Reads factors in batches from Parquet.

    Args:
        future_perf_df: Pre-calculated future performance (Date, Ticker, Future Perf)
        reader: ParquetDataReader for streaming factor data
        factor_columns: List of factor columns to analyze (auto-detected if None)
        top_pct: Percentage of top stocks to include (default: 30.0)
        bottom_pct: Percentage of bottom stocks to include (default: 30.0)
        progress_fn: Optional callback (completed, total, current_factor) for progress updates
        batch_size: Number of factors to read per batch (default: 50)

    Returns:
        DataFrame with factor analysis results (Date, factor, ret)
    """
    total_factors = len(factor_columns)
    results: List[dict] = []

    for batch_start in range(0, total_factors, batch_size):
        batch_cols = factor_columns[batch_start : batch_start + batch_size]

        batch_df = reader.read_columns(["Date", "Ticker"] + batch_cols)

        for idx, col in enumerate(batch_cols, batch_start + 1):
            merged = batch_df[["Date", "Ticker", col]].merge(
                future_perf_df, on=["Date", "Ticker"], how="inner"
            )

            factor_results = _analyze_factor_by_date(merged, col, top_pct, bottom_pct)
            results.extend(factor_results)

            if progress_fn:
                progress_fn(idx, total_factors, col)

        del batch_df

    return pd.DataFrame(results)


def _analyze_factor_by_date(
    merged_df: pd.DataFrame, factor_col: str, top_pct: float, bottom_pct: float
) -> List[dict]:
    # TODO: determine how to handle missing values, or stocks exiting/entering the universe
    valid = merged_df[[factor_col, "Future Perf", "Date"]].dropna()

    # sort once per date and factor, grab first X as topx, last X as bottomx
    valid = valid.sort_values(["Date", factor_col]).reset_index(drop=True)

    # count how many stocks in each date, per factor
    valid["group_size"] = valid.groupby("Date")[factor_col].transform("size")
    # give auto-increment rank to each stock. lowest are towards bottomx, highest to topx
    valid["rank_in_group"] = valid.groupby("Date").cumcount()

    # how many stocks to include in topx and bottomx
    top_n = (valid["group_size"] * (top_pct / 100.0)).astype(int)
    bottom_n = (valid["group_size"] * (bottom_pct / 100.0)).astype(int)

    is_bottom = valid["rank_in_group"] < bottom_n
    is_top = valid["rank_in_group"] >= (valid["group_size"] - top_n)

    # mark non-top and non-bottom stocks as 0
    perf = valid["Future Perf"].values
    valid["top_perf"] = np.where(is_top, perf, 0.0)
    valid["bottom_perf"] = np.where(is_bottom, perf, 0.0)
    valid["top_n"] = top_n
    valid["bottom_n"] = bottom_n

    # grab topx and bottomx rows per date, sum future perf % returns
    agg = valid.groupby("Date", sort=False).agg(
        top_sum=("top_perf", "sum"),
        bottom_sum=("bottom_perf", "sum"),
        top_n=("top_n", "first"),
        bottom_n=("bottom_n", "first"),
    )

    # combine topx and bottomx per date, and calculate the return metric
    agg["ret"] = (agg["top_sum"] - agg["bottom_sum"]) / (agg["top_n"] + agg["bottom_n"])
    agg["factor"] = factor_col

    # convert the dataframe to a list of dictionaries
    return agg[["factor", "ret"]].reset_index().to_dict("records")


def calculate_factor_metrics(
    results_df: pd.DataFrame,
    raw_data: pd.DataFrame,
    periods_per_year: int = 52,
) -> pd.DataFrame:
    """
    Calculate statistical metrics for each factor.
    Computes alpha, beta, t-statistic, and p-value.

    Args:
        results_df: DataFrame with factor returns (Date, factor, ret)
        raw_data: DataFrame with benchmark data from p123 api
        periods_per_year: Number of periods per year for annualization (default: 52 for weekly)

    Returns:
        DataFrame with factor metrics
    """
    benchmark = raw_data[["Date", "benchmark"]].drop_duplicates()
    merged_data = results_df.merge(benchmark, on="Date", how="inner")

    metrics = []
    unique_factors = results_df["factor"].unique()

    for col in unique_factors:
        subset = merged_data[merged_data["factor"] == col]

        valid_subset = subset[
            np.isfinite(subset["ret"]) & np.isfinite(subset["benchmark"])
        ]

        if len(valid_subset) < 2:
            continue  # Need at least 2 points for regression
        x = valid_subset["benchmark"]
        y = valid_subset["ret"]

        # Linear regression: return = alpha + beta * benchmark
        beta, alpha = np.polyfit(x, y, deg=1)

        # annualized alpha based on data frequency
        ann_alpha = 100 * ((1 + alpha) ** periods_per_year - 1)

        # T-student test
        t_stat, p_value = stats.ttest_1samp(y, popmean=0)

        metrics.append(
            {
                "column": col,
                "T Statistic": t_stat,
                "p-value": p_value,
                "beta": beta,
                "alpha": alpha,
                "annualized alpha %": ann_alpha,
            }
        )

    return pd.DataFrame(metrics)


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
    N: int = 10,
    correlation_threshold: float = 0.5,
    a_min: float = 0.5,
) -> tuple[list, dict[str, str]]:
    """
    Select N best features based on alpha and low correlation.
    Also classifies all factors into categories.

    Args:
        metrics_df: DataFrame with feature metrics
        correlation_matrix: Correlation matrix of features
        N: Number of features to select
        correlation_threshold: Maximum allowed correlation
        a_min: Minimum absolute annualized alpha %

    Returns:
        Tuple of (selected feature names, classifications dict)
        Classifications: "best", "below_alpha", "correlation_conflict", or "n_limit"
    """
    classifications = {}
    selected_features = []

    sorted_metrics = metrics_df.sort_values(
        by="annualized alpha %", key=abs, ascending=False
    )

    for feature, alpha in zip(
        sorted_metrics["column"], sorted_metrics["annualized alpha %"]
    ):
        if abs(alpha) < a_min:
            classifications[feature] = "below_alpha"
            continue

        if len(selected_features) >= N:
            classifications[feature] = "n_limit"
            continue

        if all(
            abs(correlation_matrix.loc[feature, selected]) < correlation_threshold
            for selected in selected_features
        ):
            selected_features.append(feature)
            classifications[feature] = "best"
        else:
            classifications[feature] = "correlation_conflict"

    return selected_features, classifications
