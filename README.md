## FactorMiner

FactorMiner is a tool designed to run analyses over datasets and identify the factors with highest returns relative to a benchmark.

### Installation

```bash
python -m venv venv
source venv/bin/activate      # Linux/macOS
venv\Scripts\Activate.ps1     # Windows
pip install -r requirements.txt
```

### Configuration

Create a `.env` file with the following variables:

```env
DATASET_DIR=/path/to/your/data
```

| Variable      | Description                                         |
| ------------- | --------------------------------------------------- |
| `DATASET_DIR` | Directory containing your `.parquet` dataset files. |

### Usage

```bash
streamlit run app.py
```

1. Place your `.parquet` dataset files in the `DATASET_DIR` directory
2. Select a dataset from the sidebar dropdown
3. Click on **New Analysis** to run your analysis

### Dataset Requirements

Your parquet file requires the following columns and format to work properly:

| Column Name | Format | Example |
| :--- | :--- | :--- |
| `Date` | YYYY-MM-DD | 2022-04-09 |
| `Ticker` |  | DAL |
| `__Future_Perf__` | Percentage (Basis 100) | -3.2590967 |

**Notes:**

- `__Future_Perf__` represents the percentage return of the ticker from the current date to the next rebalancing date
- `Date` has to be pre-sorted in a monotonic way

### Results Storage

Analysis results are stored in `{DATASET_DIR}/FactorMiner/`. A directory is created for each dataset, containing:

- JSON files with analysis results (one per analysis)
- A `logs/` folder with execution logs
