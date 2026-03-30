import numpy as np
from scipy.stats import rankdata, ttest_1samp

from src.core.types.models import FactorMetricResult


def weighted_ic(x: np.ndarray, y: np.ndarray, alpha: float = 4) -> float:
    mask = np.isfinite(x) & np.isfinite(y)
    x, y = x[mask], y[mask]

    if len(x) < 2:
        return np.nan

    ranked_x = (rankdata(x, method="average") - 0.5) / len(x)
    ranked_y = (rankdata(y, method="average") - 0.5) / len(y)
    w = 1 + alpha * np.abs(ranked_x - 0.5)
    cv = np.cov(ranked_x, ranked_y, aweights=w)
    denom = cv[0, 0] * cv[1, 1]
    if denom <= 0:
        return np.nan
    return cv[0, 1] / np.sqrt(denom)


def calculate_na_pct(factor_arr: np.ndarray) -> float:
    return np.isnan(factor_arr).sum() / len(factor_arr) * 100


def cumulative_return(returns: np.ndarray) -> np.ndarray:
    return np.prod(1 + returns) - 1


def annualize_return(returns: np.ndarray, periods_per_year):
    return (1 + cumulative_return(returns)) ** (periods_per_year / len(returns)) - 1


def calculate_factor_metric(
    y: np.ndarray, x: np.ndarray, periods_per_year: float
) -> FactorMetricResult:

    mask = np.isfinite(y) & np.isfinite(x)  # exclude invalid returns

    y_valid = y[mask]
    x_valid = x[mask]

    beta, alpha = np.polyfit(x_valid, y_valid, deg=1)
    annualized_alpha = 100 * ((1 + alpha) ** periods_per_year - 1)

    t_stat, _ = ttest_1samp(y_valid, popmean=0)

    return {"beta": beta, "t_stat": t_stat, "annualized_alpha_pct": annualized_alpha}
