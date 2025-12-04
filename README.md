## How to Run

### Prerequisites
- Python 3

#### 1. Create a Virtual Environment
```bash
python -m venv venv
```

#### 2. Activate the Virtual Environment

**Linux/macOS:**
```bash
source venv/bin/activate
```

**Windows (PowerShell):**
```powershell
venv\Scripts\Activate.ps1
```

#### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

#### 4. Configure Environment (Optional)
Create a `.env` file in the project root:
```env
INTERNAL_APP=false
```

#### 5. Run the Application

streamlit run app.py

The app will open in http://localhost:8501/
