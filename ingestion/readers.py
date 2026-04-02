"""
readers.py
==========
MK Intel Ingestion — Multi-Format File Readers

Handles reading company data files into a normalized pandas DataFrame.
Supports: CSV, TSV, JSON (array / records / JSONL), XLSX, Parquet.

Each reader:
    - Accepts a file path
    - Returns a pandas DataFrame with original column names preserved,
      except leading/trailing whitespace is stripped from column names
    - Raises FileNotFoundError if the file does not exist
    - Raises ValueError for unsupported formats or unreadable files
    - Never modifies column values — that is the normalizer's job

Notes:
    - .txt files are assumed to be tab-separated. This is a convention,
      not true format detection. If your .txt file uses a different
      delimiter, rename it or use read_csv() with sep= directly.
    - JSON shape B (dict of columns) is supported for pandas-oriented
      exports only. Nested dicts with a top-level wrapper key are not
      yet supported — flatten before ingesting.

Public API
----------
    read_file(path)          Auto-detects format and dispatches to correct reader.
    read_csv(path)           Reads CSV files.
    read_tsv(path)           Reads TSV files.
    read_json(path)          Reads JSON files (array, records dict, or JSONL).
    read_xlsx(path)          Reads XLSX files (first sheet by default).
    read_parquet(path)       Reads Parquet files.
    detect_format(path)      Returns the detected format string for a given path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd


# ── Supported formats ─────────────────────────────────────────────────────────

SUPPORTED_FORMATS = {
    ".csv":     "csv",
    ".tsv":     "tsv",
    ".txt":     "tsv",   # .txt files are often tab-separated exports
    ".json":    "json",
    ".jsonl":   "jsonl",
    ".ndjson":  "jsonl",
    ".xlsx":    "xlsx",
    ".xls":     "xlsx",
    ".parquet": "parquet",
}


# ── Format detection ──────────────────────────────────────────────────────────

def detect_format(path: Path) -> str:
    """
    Detect the file format from the file extension.

    Args:
        path : Path to the file.

    Returns:
        Format string: "csv", "tsv", "json", "jsonl", "xlsx", "parquet".

    Raises:
        ValueError : Unsupported file extension.
    """
    ext = Path(path).suffix.lower()
    fmt = SUPPORTED_FORMATS.get(ext)
    if fmt is None:
        raise ValueError(
            f"Unsupported file format: '{ext}'\n"
            f"Supported extensions: {list(SUPPORTED_FORMATS.keys())}"
        )
    return fmt


# ── Individual readers ────────────────────────────────────────────────────────

def read_csv(path: Path) -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame.

    Handles common encoding issues (UTF-8 with BOM, Latin-1).
    Strips leading/trailing whitespace from column names.

    Args:
        path : Path to the CSV file.

    Returns:
        DataFrame with original column names preserved.

    Raises:
        FileNotFoundError : File does not exist.
        ValueError        : File cannot be parsed as CSV.
    """
    path = Path(path)
    _assert_exists(path)

    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            df = pd.read_csv(path, encoding=encoding, low_memory=False)
            df.columns = df.columns.str.strip()
            _assert_not_empty(df, path)
            return df
        except UnicodeDecodeError:
            continue
        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV file is empty or has no data rows: {path}")
        except pd.errors.ParserError as e:
            raise ValueError(f"CSV file could not be parsed: {path}\n{e}")

    raise ValueError(f"Could not decode CSV file with any supported encoding: {path}")


def read_tsv(path: Path) -> pd.DataFrame:
    """
    Read a TSV (tab-separated) file into a DataFrame.

    Args:
        path : Path to the TSV file.

    Returns:
        DataFrame with original column names preserved.

    Raises:
        FileNotFoundError : File does not exist.
        ValueError        : File cannot be parsed as TSV.
    """
    path = Path(path)
    _assert_exists(path)

    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            df = pd.read_csv(path, sep="\t", encoding=encoding, low_memory=False)
            df.columns = df.columns.str.strip()
            _assert_not_empty(df, path)
            return df
        except UnicodeDecodeError:
            continue
        except pd.errors.EmptyDataError:
            raise ValueError(f"TSV file is empty or has no data rows: {path}")
        except pd.errors.ParserError as e:
            raise ValueError(f"TSV file could not be parsed: {path}\n{e}")

    raise ValueError(f"Could not decode TSV file with any supported encoding: {path}")


def read_json(path: Path) -> pd.DataFrame:
    """
    Read a JSON file into a DataFrame.

    Handles three common JSON shapes:
        Shape A — Array of records:
            [{"customer_id": "123", "age": 35}, ...]

        Shape B — Records dict (pandas default orient):
            {"customer_id": {"0": "123"}, "age": {"0": 35}}

        Shape C — Newline-delimited JSON / JSONL:
            {"customer_id": "123", "age": 35}
            {"customer_id": "124", "age": 42}

    Args:
        path : Path to the JSON or JSONL file.

    Returns:
        DataFrame with original column names preserved.

    Raises:
        FileNotFoundError : File does not exist.
        ValueError        : File cannot be parsed as any supported JSON shape.
    """
    path = Path(path)
    _assert_exists(path)

    # ── Try Shape C first (JSONL / newline-delimited) ─────────────────────────
    # Check if the file extension signals JSONL
    if path.suffix.lower() in (".jsonl", ".ndjson"):
        return _read_jsonl(path)

    # ── Try to load the full file as JSON ─────────────────────────────────────
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
    except UnicodeDecodeError:
        with open(path, "r", encoding="latin-1") as f:
            content = f.read().strip()

    if not content:
        raise ValueError(f"JSON file is empty: {path}")

    # ── Shape C detection: first non-whitespace char is '{' but not a single obj
    # Try JSONL if the content looks like multiple JSON objects
    if content.startswith("{"):
        lines = [l.strip() for l in content.splitlines() if l.strip()]
        if len(lines) > 1:
            try:
                records = [json.loads(line) for line in lines]
                df = pd.DataFrame(records)
                _assert_not_empty(df, path)
                return df
            except json.JSONDecodeError:
                pass  # Fall through to single-object parse

    # ── Shape A or B: parse as standard JSON ──────────────────────────────────
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON file could not be parsed: {path}\n{e}")

    # Shape A — list of records
    if isinstance(data, list):
        if len(data) == 0:
            raise ValueError(f"JSON array is empty: {path}")
        df = pd.DataFrame(data)
        _assert_not_empty(df, path)
        return df

    # Shape B — dict of columns
    if isinstance(data, dict):
        try:
            df = pd.DataFrame(data)
            _assert_not_empty(df, path)
            return df
        except ValueError as e:
            raise ValueError(
                f"JSON dict could not be converted to DataFrame: {path}\n{e}"
            )

    raise ValueError(
        f"Unrecognized JSON structure in {path}. "
        f"Expected array of records, records dict, or newline-delimited JSON."
    )


def read_xlsx(
    path: Path,
    sheet_name: Optional[str | int] = 0,
) -> pd.DataFrame:
    """
    Read an XLSX file into a DataFrame.

    By default reads the first sheet (index 0).
    If the file has multiple sheets, logs a warning and uses the first.

    Args:
        path       : Path to the XLSX file.
        sheet_name : Sheet name or index to read. Default: 0 (first sheet).

    Returns:
        DataFrame with original column names preserved.

    Raises:
        FileNotFoundError : File does not exist.
        ValueError        : File cannot be parsed as XLSX.
    """
    path = Path(path)
    _assert_exists(path)

    try:
        xl = pd.ExcelFile(path)
        sheet_names = xl.sheet_names

        # ── Validate sheet_name before use ───────────────────────────────────
        if isinstance(sheet_name, int):
            if sheet_name < 0 or sheet_name >= len(sheet_names):
                raise ValueError(
                    f"Sheet index {sheet_name} out of range. "
                    f"File has {len(sheet_names)} sheet(s): {sheet_names}"
                )
            selected_sheet = sheet_names[sheet_name]
        else:
            if sheet_name not in sheet_names:
                raise ValueError(
                    f"Sheet name '{sheet_name}' not found. "
                    f"Available sheets: {sheet_names}"
                )
            selected_sheet = sheet_name

        if len(sheet_names) > 1:
            print(
                f"[readers] Warning: XLSX file has {len(sheet_names)} sheets: "
                f"{sheet_names}. Reading sheet '{selected_sheet}'. "
                f"Pass sheet_name= to read a different sheet."
            )

        df = pd.read_excel(path, sheet_name=sheet_name)
        df.columns = df.columns.str.strip().astype(str)
        _assert_not_empty(df, path)
        return df

    except ValueError as e:
        raise ValueError(f"XLSX file could not be parsed: {path}\n{e}")
    except Exception as e:
        raise ValueError(f"Error reading XLSX file: {path}\n{e}")


def read_parquet(path: Path) -> pd.DataFrame:
    """
    Read a Parquet file into a DataFrame.

    Args:
        path : Path to the Parquet file.

    Returns:
        DataFrame with original column names preserved.

    Raises:
        FileNotFoundError : File does not exist.
        ValueError        : File cannot be parsed as Parquet.
    """
    path = Path(path)
    _assert_exists(path)

    try:
        df = pd.read_parquet(path)
        _assert_not_empty(df, path)
        return df
    except Exception as e:
        raise ValueError(f"Parquet file could not be read: {path}\n{e}")


# ── Main dispatcher ───────────────────────────────────────────────────────────

def read_file(
    path: Path,
    sheet_name: Optional[str | int] = 0,
) -> pd.DataFrame:
    """
    Auto-detect file format and read into a DataFrame.

    This is the primary entry point for the ingestion pipeline.
    Format is detected from the file extension.

    Args:
        path       : Path to the data file.
        sheet_name : For XLSX files — sheet name or index. Default: 0.

    Returns:
        DataFrame with original column names and values preserved.

    Raises:
        FileNotFoundError : File does not exist.
        ValueError        : Unsupported format or unreadable file.
    """
    path = Path(path)
    fmt  = detect_format(path)

    readers = {
        "csv":     lambda: read_csv(path),
        "tsv":     lambda: read_tsv(path),
        "json":    lambda: read_json(path),
        "jsonl":   lambda: read_json(path),
        "xlsx":    lambda: read_xlsx(path, sheet_name=sheet_name),
        "parquet": lambda: read_parquet(path),
    }

    print(f"[readers] Reading {fmt.upper()} file: {path.name}")
    df = readers[fmt]()
    print(f"[readers] Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


# ── Internal helpers ──────────────────────────────────────────────────────────

def _assert_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")


def _assert_not_empty(df: pd.DataFrame, path: Path) -> None:
    if df.empty:
        raise ValueError(f"File loaded but contains no data rows: {path}")
    if len(df.columns) == 0:
        raise ValueError(f"File loaded but contains no columns: {path}")


def _read_jsonl(path: Path) -> pd.DataFrame:
    """Read a newline-delimited JSON file with malformed line reporting."""
    records      = []
    total_lines  = 0
    skipped      = 0

    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                skipped += 1
                print(f"[readers] Warning: skipping malformed line {line_num}: {e}")

    if skipped > 0:
        malformed_rate = skipped / total_lines if total_lines > 0 else 1.0
        print(
            f"[readers] JSONL load summary: {len(records)} valid, "
            f"{skipped} skipped ({malformed_rate:.1%} malformed)"
        )
        if malformed_rate > 0.10:
            print(
                f"[readers] Warning: more than 10% of JSONL lines were malformed. "
                f"Review the source file before proceeding."
            )

    if not records:
        raise ValueError(f"JSONL file contains no valid records: {path}")

    df = pd.DataFrame(records)
    _assert_not_empty(df, path)
    return df
