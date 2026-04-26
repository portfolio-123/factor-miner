import math

import numpy as np
from scipy.stats import rankdata, ttest_1samp
from typing import Any

from src.core.types.models import FactorMetricResult


def weighted_ic(x: np.ndarray, y: np.ndarray, alpha=4.0) -> float:
    ranked_x = (rankdata(x, method="average") - 0.5) / len(x)
    ranked_y = (rankdata(y, method="average") - 0.5) / len(y)
    w = 1 + alpha * np.abs(ranked_x - 0.5)
    cv = np.cov(ranked_x, ranked_y, aweights=w)
    denom = cv[0, 0] * cv[1, 1]
    if denom <= 0:
        return math.nan
    return float(cv[0, 1] / np.sqrt(denom))


def calculate_na_pct(factor_arr: np.ndarray) -> float:
    return float(np.isnan(factor_arr).sum()) / len(factor_arr) * 100.0


def cumulative_return(returns: np.ndarray) -> np.ndarray:
    return np.prod(1 + returns) - 1  # type: ignore


def annualize_return(returns: np.ndarray, periods_per_year: float) -> float:
    return float((1 + cumulative_return(returns)) ** (periods_per_year / len(returns)) - 1)


def calculate_factor_metric(y: np.ndarray, x: np.ndarray, periods_per_year: float) -> FactorMetricResult:
    beta, alpha = np.polyfit(x, y, deg=1)
    annualized_alpha = 100 * ((1 + alpha) ** periods_per_year - 1)

    t_stat: Any = ttest_1samp(y, popmean=0)[0]

    return {"beta": float(beta), "t_stat": float(t_stat), "annualized_alpha_pct": float(annualized_alpha)}
