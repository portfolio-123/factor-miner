import streamlit as st


def about() -> None:
    st.title("Factor Miner")

    st.markdown(
        f"""
Factor Miner is a Portfolio123 tool designed to run analyses over datasets and identify the factors with highest returns relative to a benchmark. It automates the process of analyzing factor performance,
determining alpha and beta, and selecting
uncorrelated factors in your portfolio.

You can start in the [New Analysis](?page=create&fl_id={st.query_params.get("fl_id")}) page to configure your analysis parameters, run the
factor evaluation against historical data, and review the results to identify
which factors demonstrate consistent outperformance with statistical significance.
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
