import streamlit as st

from src.core.environment import P123_BASE_URL


def about() -> None:
    st.title("Factor Miner")

    fl_id = st.query_params.get("fl_id")
    st.markdown(
        f"""
Factor Miner is tightly coupled to Portfolio 123's [Factor List]({P123_BASE_URL}/sv/factorList/{fl_id}) feature—you need to generate a dataset there before running any analysis here.

Factor Miner is a Portfolio123 tool designed to run analyses over datasets and identify the factors with highest returns relative to a benchmark. It automates the process of analyzing factor performance,
determining alpha and beta, and selecting
uncorrelated factors in your portfolio.

You can start in the [New Analysis](/create?fl_id={fl_id}) page. It will run using your current generated dataset and you'll be able to modify the following params:
"""
    )
    st.markdown("### Analysis Parameters")
    st.markdown(
        """
When configuring a new analysis, you can adjust the following parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| **Benchmark Ticker** | SPY:USA | The ticker symbol used for benchmark comparison when calculating alpha and beta |
| **Min Absolute Alpha (%)** | 0.5 | Minimum alpha threshold - factors with absolute alpha below this are filtered out |
| **Top X (%)** | 20.0 | Percentage of top-ranked stocks to include in the long portfolio |
| **Bottom X (%)** | 20.0 | Percentage of bottom-ranked stocks to include in the short portfolio |
| **Correlation Threshold** | 0.5 | Maximum allowed correlation between selected factors to ensure diversification |
| **N Factors** | 10 | Maximum number of factors to select in the final result set |
"""
    )

    st.markdown("### Analysis Results")
    st.markdown(
        """
After running an analysis, you'll be able to access the following results:

- **Best Factors** - The factors with the highest absolute annualized alpha.
- **All Factors** - All factors with their respective metrics, despite alpha or correlation filters.
- **Correlation Matrix** - The correlation matrix of the factors. Only shown for the best factors.
"""
    )
