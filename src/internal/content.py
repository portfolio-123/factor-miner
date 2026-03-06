# internal mode about me page

def get_about_intro(fl_id: str, p123_link: str) -> str:
    return f"""
FactorMiner is tightly coupled to Portfolio 123's [Factor List]({p123_link}) feature—you need to generate a dataset there before running any analysis here.

FactorMiner is a Portfolio123 tool designed to run analyses over datasets and identify the factors with highest returns relative to a benchmark. It automates the process of analyzing factor performance,
determining alpha and beta, and selecting
uncorrelated factors in your portfolio.

You can start in the [New Analysis](/create?fl_id={fl_id}) page. It will run using your current generated dataset and you'll be able to modify the following params:
"""
