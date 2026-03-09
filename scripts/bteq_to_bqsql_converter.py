"""
bteq_to_bqsql_converter.py
--------------------------
Automates conversion of Teradata BTEQ scripts to Google BigQuery SQL.
Part of the Teradata → GCP Migration Project.

Author: [Your Name]
"""

import re
import os
import argparse
from pathlib import Path


# ─── Syntax Mapping Rules ────────────────────────────────────────────────────

BTEQ_PATTERNS = [
    # Remove BTEQ session commands
    (r'\.LOGON\s+\S+;\s*\n?',           '',                         'Strip .LOGON'),
    (r'\.LOGOFF\s*;\s*\n?',             '',                         'Strip .LOGOFF'),
    (r'\.QUIT\s*;\s*\n?',               '',                         'Strip .QUIT'),
    (r'\.SET\s+\w+\s+\S+;\s*\n?',       '',                         'Strip .SET directives'),
    (r'\.IF\s+.*?\.QUIT\s+\d+\s*;',     '',                         'Strip error-exit blocks'),

    # Data type conversions
    (r'\bBYTEINT\b',                    'INT64',                    'BYTEINT → INT64'),
    (r'\bSMALLINT\b',                   'INT64',                    'SMALLINT → INT64'),
    (r'\bINTEGER\b',                    'INT64',                    'INTEGER → INT64'),
    (r'\bBIGINT\b',                     'INT64',                    'BIGINT → INT64'),
    (r'\bFLOAT\b',                      'FLOAT64',                  'FLOAT → FLOAT64'),
    (r'\bVARCHAR\((\d+)\)',             r'STRING',                  'VARCHAR → STRING'),
    (r'\bCHAR\((\d+)\)',                r'STRING',                  'CHAR → STRING'),

    # Null-handling functions
    (r'\bZEROIFNULL\s*\(([^)]+)\)',     r'IFNULL(\1, 0)',           'ZEROIFNULL → IFNULL'),
    (r'\bNULLIFZERO\s*\(([^)]+)\)',     r'NULLIF(\1, 0)',           'NULLIFZERO → NULLIF'),
    (r'\bCOALESCE\b',                   'COALESCE',                 'COALESCE (keep as-is)'),

    # Date functions
    (r'\bTD_MONTH_BEGIN\s*\(([^)]+)\)', r'DATE_TRUNC(\1, MONTH)',   'TD_MONTH_BEGIN → DATE_TRUNC'),
    (r'\bTD_YEAR_BEGIN\s*\(([^)]+)\)',  r'DATE_TRUNC(\1, YEAR)',    'TD_YEAR_BEGIN → DATE_TRUNC'),
    (r'\bCURRENT_DATE\s*-\s*(\d+)',     r'DATE_SUB(CURRENT_DATE(), INTERVAL \1 DAY)', 'Date subtraction'),
    (r'\bADD_MONTHS\s*\(([^,]+),\s*(\d+)\)', r'DATE_ADD(\1, INTERVAL \2 MONTH)', 'ADD_MONTHS → DATE_ADD'),

    # Table qualifiers
    (r'\bCREATE VOLATILE TABLE\b',      'CREATE TEMP TABLE',        'Volatile → Temp'),
    (r'\bON COMMIT PRESERVE ROWS\b',    '',                         'Remove TD-specific clause'),
    (r'\bPRIMARY INDEX\s*\([^)]+\)',     '',                         'Remove PRIMARY INDEX'),
    (r'\bWITH DATA\b',                  '',                         'Remove WITH DATA'),

    # Sampling
    (r'\bSAMPLE\s+(\d+)\b',            r'LIMIT \1',                'SAMPLE → LIMIT'),

    # String functions
    (r'\bINDEX\s*\(([^,]+),\s*([^)]+)\)', r'STRPOS(\1, \2)',       'INDEX → STRPOS'),
    (r'\bOTRANSLATE\b',                 'TRANSLATE',                'OTRANSLATE → TRANSLATE'),
    (r"''\s*\|\|",                      "CONCAT('',",               'String concat style'),
]


def convert_bteq_to_bqsql(bteq_content: str, source_file: str = "") -> str:
    """
    Convert a BTEQ script string to BigQuery SQL.

    Args:
        bteq_content: Raw BTEQ script content
        source_file:  Optional source filename for header comments

    Returns:
        Converted BigQuery SQL string
    """
    sql = bteq_content

    header = f"""-- ============================================================
-- Converted from BTEQ: {source_file or 'unknown'}
-- Converter : bteq_to_bqsql_converter.py
-- Target    : Google BigQuery (GCP)
-- ============================================================\n\n"""

    conversion_notes = []

    for pattern, replacement, description in BTEQ_PATTERNS:
        original = sql
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE | re.DOTALL)
        if sql != original:
            conversion_notes.append(f"  ✔ {description}")

    # Clean up extra blank lines left from removed directives
    sql = re.sub(r'\n{3,}', '\n\n', sql).strip()

    if conversion_notes:
        notes_block = "-- Conversions applied:\n" + "\n".join(
            f"-- {n}" for n in conversion_notes
        ) + "\n\n"
    else:
        notes_block = "-- No automatic conversions applied.\n\n"

    return header + notes_block + sql


def process_directory(input_dir: str, output_dir: str):
    """
    Batch convert all .bteq files in input_dir → .sql files in output_dir.
    """
    input_path  = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    bteq_files = list(input_path.glob("**/*.bteq"))
    if not bteq_files:
        print(f"⚠  No .bteq files found in {input_dir}")
        return

    success, failed = 0, 0

    for bteq_file in bteq_files:
        try:
            content   = bteq_file.read_text(encoding="utf-8")
            converted = convert_bteq_to_bqsql(content, bteq_file.name)

            out_file  = output_path / bteq_file.with_suffix(".sql").name
            out_file.write_text(converted, encoding="utf-8")

            print(f"  ✅  {bteq_file.name} → {out_file.name}")
            success += 1
        except Exception as e:
            print(f"  ❌  {bteq_file.name} failed: {e}")
            failed += 1

    print(f"\n{'─'*50}")
    print(f"  Conversion complete: {success} succeeded, {failed} failed")
    print(f"  Output directory   : {output_path.resolve()}")


# ─── CLI Entry Point ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert Teradata BTEQ scripts to Google BigQuery SQL"
    )
    parser.add_argument("--input",  required=True, help="Directory containing .bteq files")
    parser.add_argument("--output", required=True, help="Directory to write .sql output files")
    args = parser.parse_args()

    print(f"\n🔄  BTEQ → BigQuery SQL Converter")
    print(f"    Input  : {args.input}")
    print(f"    Output : {args.output}\n")
    process_directory(args.input, args.output)
