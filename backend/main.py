import os
import zipfile
import uuid
import re
import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text, inspect
from pydantic import BaseModel

# --- LOGGING & CONFIG ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pg-data-hub")

# Update with your actual credentials
# DATABASE_URL = "postgresql://postgres:password@localhost:5432/your_db"
DATABASE_URL = "postgresql://app_user:Wolfie1234@13.48.147.27:5432/bi_data"
engine = create_engine(DATABASE_URL, pool_size=20, max_overflow=40)

# Server-side registry to prevent path traversal/tampering
FILE_REGISTRY: Dict[str, str] = {}

app = FastAPI(title="PostgreSQL Data Hub API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- TYPE HIERARCHY & PROMOTION ---

TYPE_HIERARCHY = {
    "BOOLEAN": 1,
    "BIGINT": 2,
    "DOUBLE PRECISION": 3,
    "TIMESTAMP": 4,
    "TEXT": 5
}

def get_promoted_type(current_type: str, new_type: str) -> str:
    """Calculates the 'wider' type to prevent data loss during schema evolution."""
    curr = str(current_type).upper()
    nxt = str(new_type).upper()
    
    # Map SQLAlchemy/Standard names to our internal hierarchy
    mapping = {"INTEGER": "BIGINT", "VARCHAR": "TEXT", "NUMERIC": "DOUBLE PRECISION", "DATETIME": "TIMESTAMP"}
    curr = next((v for k, v in mapping.items() if k in curr), curr)
    nxt = next((v for k, v in mapping.items() if k in nxt), nxt)

    return nxt if TYPE_HIERARCHY.get(nxt, 0) > TYPE_HIERARCHY.get(curr, 0) else curr

# --- UTILITIES ---

def sanitize(name: str) -> str:
    """Ensures names are valid PostgreSQL identifiers and handles Turkish characters."""
    if not name: return "unnamed"
    name = str(name).lower().strip()
    name = name.translate(str.maketrans("öçşğüı", "ocsgui"))
    name = re.sub(r'[^a-z0-9]', '_', name)
    return re.sub(r'_+', '_', name).strip('_')

def get_excel_engine(filename: str):
    ext = filename.split('.')[-1].lower()
    return {'xlsb': 'pyxlsb', 'xls': 'xlrd'}.get(ext, 'openpyxl')

def infer_pg_type(series: pd.Series) -> str:
    """Samples data to determine optimal PostgreSQL column type."""
    sample = series.dropna().head(1000)
    if sample.empty: return "TEXT"
    
    if pd.api.types.is_bool_dtype(sample): return "BOOLEAN"
    
    if pd.api.types.is_numeric_dtype(sample):
        return "DOUBLE PRECISION" if any(sample % 1 != 0) else "BIGINT"
    
    try:
        if pd.api.types.is_datetime64_any_dtype(sample) or \
           pd.to_datetime(sample.astype(str), errors='coerce').notnull().all():
            return "TIMESTAMP"
    except: pass
    
    return "TEXT"

# --- API MODELS ---

class ProcessRequest(BaseModel):
    import_id: str
    table_name: str
    selected_sheet: str
    primary_keys: Optional[List[str]] = None

# --- ENDPOINTS ---

@app.post("/upload")
async def upload_zip(file: UploadFile = File(...)):
    """
    1. Extracts table name from zip file name.
    2. Parses ZIP to find available sheets across all files for the dropdown.
    """
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a ZIP.")

    import_id = str(uuid.uuid4())
    temp_path = f"storage/{import_id}.zip"
    os.makedirs("storage", exist_ok=True)
    
    with open(temp_path, "wb") as buffer:
        buffer.write(await file.read())
    
    FILE_REGISTRY[import_id] = temp_path
    
    all_sheets = set()
    try:
        with zipfile.ZipFile(temp_path, 'r') as z:
            excels = [f for f in z.namelist() if f.endswith(('.xlsx', '.xls', '.xlsb', '.xlsm'))]
            for fname in excels:
                with z.open(fname) as f:
                    xl = pd.ExcelFile(f, engine=get_excel_engine(fname))
                    all_sheets.update(xl.sheet_names)
    except Exception as e:
        logger.error(f"Discovery error: {e}")
        raise HTTPException(status_code=500, detail="Failed to parse ZIP contents.")

    return {
        "import_id": import_id,
        "suggested_table": sanitize(file.filename.replace('.zip', '')),
        "available_sheets": list(all_sheets)
    }

@app.post("/process")
async def process_data(req: ProcessRequest):
    """
    The main engine: Handles dilation, promotion, cleaning (NaN -> None), 
    and atomic per-file transactions.
    """
    file_path = FILE_REGISTRY.get(req.import_id)
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Import session expired.")

    safe_table = sanitize(req.table_name)
    summary = {"success": [], "errors": [], "skipped": []}
    
    with zipfile.ZipFile(file_path, 'r') as z:
        excel_files = [f for f in z.namelist() if f.endswith(('.xlsx', '.xls', '.xlsb', '.xlsm'))]
        
        for filename in excel_files:
            # Transaction per file ensures atomicity
            with engine.begin() as conn:
                try:
                    with z.open(filename) as f:
                        xl = pd.ExcelFile(f, engine=get_excel_engine(filename))
                        if req.selected_sheet not in xl.sheet_names:
                            summary["skipped"].append({"file": filename, "reason": "Sheet not found"})
                            continue
                        
                        df = pd.read_excel(xl, sheet_name=req.selected_sheet)
                        if df.empty:
                            summary["skipped"].append({"file": filename, "reason": "Empty data"})
                            continue

                        # Clean headers and data
                        df.columns = [sanitize(c) for c in df.columns]
                        df = df.replace({np.nan: None}) # Crucial for Postgres NULL

                        # Check for Dilation (missing columns) & Promotion (type changes)
                        inspector = inspect(engine)
                        if inspector.has_table(safe_table):
                            existing = {c['name']: str(c['type']) for c in inspector.get_columns(safe_table)}
                            
                            for col in df.columns:
                                detected = infer_pg_type(df[col])
                                if col not in existing:
                                    conn.execute(text(f'ALTER TABLE "{safe_table}" ADD COLUMN "{col}" {detected}'))
                                    logger.info(f"Dilation: Added {col} ({detected}) to {safe_table}")
                                else:
                                    # Promotion logic
                                    current_type = existing[col]
                                    promoted = get_promoted_type(current_type, detected)
                                    if promoted != current_type and "TEXT" not in current_type:
                                        conn.execute(text(f'ALTER TABLE "{safe_table}" ALTER COLUMN "{col}" TYPE {promoted} USING "{col}"::{promoted}'))
                                        logger.info(f"Promotion: {col} updated to {promoted}")

                        # Insert Data
                        df.to_sql(safe_table, conn, if_exists='append', index=False, method='multi')
                        summary["success"].append(filename)
                        del df # Explicit memory release

                except Exception as e:
                    summary["errors"].append({"file": filename, "error": str(e)})
                    logger.error(f"Failed file {filename}: {e}")

    # Cleanup storage
    if os.path.exists(file_path):
        os.remove(file_path)
    FILE_REGISTRY.pop(req.import_id, None)

    return summary

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)