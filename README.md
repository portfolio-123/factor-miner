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

**Windows (Command Prompt):**
```cmd
venv\Scripts\activate
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
```bash
voila app.ipynb
```

The app will open in http://localhost:8866/

### Troubleshooting

#### Windows PowerShell Execution Policy Error
If you encounter an error about script execution being disabled:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```
