import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.internal.links import p123_link


def about() -> None:
    st.title("About FactorMiner")

    fl_id = st.query_params.get("fl_id")
    assert fl_id

    st.markdown("""
FactorMiner is a tool designed by Portfolio123 to analyze datasets and identify high-alpha factors. It automates univariate (single) factor performance analysis by estimating benchmark Alpha and Beta, tail-weighted information coefficient (IC), underlying t-statistics, factor correlations, and high and low quantile returns.

Based on your ranking and threshold settings, FactorMiner isolates a refined list of "best factors”. These results can be integrated into Portfolio123 Ranking Systems, Screens, and AIFactor, or exported for use in your own proprietary research and trading workflows. 
        """)

    if INTERNAL_MODE:
        st.warning(
            "Note: FactorMiner is tightly integrated with Portfolio123's Factor List feature. A dataset must be generated there before running any analysis in FactorMiner."
        )

    st.markdown(f"""
**For comprehensive FactorMiner documentation, please visit our [Knowledgebase](https://portfolio123.customerly.help/en/articles/53980-factorminer).**

## Quick Start
To begin, click on [New Analysis](/create?fl_id={fl_id}) to open the Create Analysis page. You can customize your analysis using the following parameters:

## Portfolio Settings

| Parameter | Default | Description |
| :--- | :--- | :--- |
| **Rank By** | Alpha | The metric used to rank factors (Annualized Alpha or IC) |
| **Factor Sorting** | Auto-detect direction | Determines the sort direction for each factor. By default, sort direction is assigned automatically based on IC analysis to ensure IC > 0. Disabling auto-assignment allows manual sort direction configuration (not recommended). |
| **High Quantile (%)** | 10.0 | Percentage of top-ranked stocks included in the high quantile (H) portfolio. If set to 0, only the low quantile is considered and factors are ranked using short-only portfolio construction logic. |
| **Low Quantile (%)** | 10.0 | Percentage of bottom-ranked stocks included in the low quantile (L) portfolio. If set to 0, only the high quantile is considered and factors are ranked using long-only portfolio construction logic. |
| **Max. Return %** | 200 | For each date, excludes any stock whose return exceeds this threshold. |

## Analysis Filters

Under **Analysis Filters**, specify the criteria a factor must meet to be included in the final "best factors" selection: 

| Parameter | Default | Description |
| :--- | :--- | :--- |
| **Min. Alpha** | 0.50 | The minimum Alpha required for a factor to be selected. For "Low-only" portfolios, this acts as a maximum threshold. |
| **Min. IC** | 0.01 | Minimum information coefficient threshold for factor selection. |
| **Max. Factors** | 10.0 | Maximum number of factors to include in the final "best factors" list. |
| **Max. NA (%)** | 40.0 | The maximum allowable percentage of missing data points across the dataset. |
| **Correlation Threshold** | 0.50 | Maximum allowed correlation between selected factors, ensuring diversification. |

## Analysis Results

Once the analysis is complete, you can explore your data through three views: 
- **Best Factors** - A list of factors with the highest (or lowest) annualized alpha or highest IC that satisfy all filter requirements. 
- **All Factors** -  A complete list of factors ranked by annualized alpha (or IC) and color-coded based on filter violations.
- **Correlations** - The correlation matrix for the "best factors" selection, along with a list of pairwise correlation conflicts that led to factor exclusions. 

### Methodology Notes
- **Portfolio Construction**: Alpha and Beta are calculated and ranked based on the portfolio construction method selected. For high-only (H) and high-low (H-L) construction, higher Alphas are preferred.
- **H−L Logic**: H−L construction assumes equal weighting of long positions in the H quantile and short positions in the L quantile, held until the next rebalancing date (as determined by dataset frequency). 
- **Low-only (L) Logic**: For low-only (L) construction, lower Alphas are preferred; however, portfolio statistics are still calculated using long-only logic (buying the low quantile).

## Further Processing
Each result table can be downloaded as a CSV or copied to the clipboard for further use. For example, you can extract the formula, name, and tag columns, add a column populated with "Skip" and another with "Date," and import the resulting feature set table into AIFactor. 
        """)
