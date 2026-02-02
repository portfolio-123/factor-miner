## Factor Miner

```bash
python -m venv venv
source venv/bin/activate      # Linux/macOS
venv\Scripts\Activate.ps1     # Windows
pip install -r requirements.txt
```

Create a `.env` file:
```env
FACTOR_LIST_DIR=/path/to/factor/list
JWT_SECRET=your_jwt_secret
P123_BASE_URL=https://your-p123-url
API_BASE_URL=https://your-api-url
```

```bash
streamlit run app.py
```
