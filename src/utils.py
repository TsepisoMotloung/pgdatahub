import re
from datetime import datetime

# reuse a cleaning behavior similar to root main.py
TR_MAPPING = str.maketrans("ıİğĞüÜşŞöÖçÇ", "iIgGuUsSoOcC")


def clean_text(text: str) -> str:
    """Normalize text: lowercase, underscores, ASCII-like, no special chars.

    - remove extension if present
    - replace special chars with underscore
    - collapse multiple underscores
    - prefix columns if starting with digit
    """
    if text is None:
        return ""

    if "." in text and not text.startswith("."):
        text = text.split(".")[0]

    text = str(text)
    text = text.replace(".", "_")
    text = text.translate(TR_MAPPING).lower()
    text = re.sub(r"[^a-z0-9_]", "_", text)
    text = re.sub(r"_+", "_", text)
    text = text.strip("_")
    if text and text[0].isdigit():
        text = "col_" + text
    return text


def normalize_dataframe_columns(df):
    df = df.copy()
    df.columns = [clean_text(c) for c in df.columns]
    return df


def utcnow_iso():
    return datetime.utcnow().isoformat(sep=" ", timespec="seconds")
