import argparse
from pathlib import Path
import sys

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent.parent
EXPECTED_DIR = ROOT_DIR / "expected_outputs" / "expected_hi_small"
OUTPUTS_DIR = ROOT_DIR / "outputs" / "client_0"

def get_queries_spec(expected_dir) -> dict:
    query_specs = {
    "q1": {
        "expected": expected_dir / "q1_results.csv",
        "actual": OUTPUTS_DIR / "q1_result.csv",
        "columns": ["account", "to_account", "amount"],
        "types": {"account": "str", "to_account": "str", "amount": "float"},
        "sort": ["account", "to_account", "amount"],
        "atol": 1e-2,
    },
    "q2": {
        "expected": expected_dir / "q2_results.csv",
        "actual": OUTPUTS_DIR / "q2_result.csv",
        "columns": ["bank_name", "account", "amount"],
        "types": {"bank_name": "str", "account": "str", "amount": "float"},
        "sort": ["bank_name", "account", "amount"],
        "atol": 1e-2,
    },
    "q3": {
        "expected": expected_dir / "q3_results.csv",
        "actual": OUTPUTS_DIR / "q3_result.csv",
        "columns": ["account", "amount"],
        "types": {"account": "str", "amount": "float"},
        "sort": ["account", "amount"],
        "atol": 1e-2,
    },
    "q4": {
        "expected": expected_dir / "q4_results.csv",
        "actual": OUTPUTS_DIR / "q4_result.csv",
        "columns": ["bank", "account"],
        "types": {"bank": "str", "account": "str"},
        "sort": ["bank", "account"],
        "atol": 1e-2,
    },
    "q5": {
        "expected": expected_dir / "q5_results.csv",
        "actual": OUTPUTS_DIR / "q5_result.csv",
        "columns": ["count"],
        "types": {"count": "int"},
        "sort": ["count"],
        "atol": 1e-2,
    },
    }
    return query_specs


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower()
    return df


def _apply_types(df: pd.DataFrame, types: dict) -> pd.DataFrame:
    for column, dtype in types.items():
        if dtype == "str":
            df[column] = df[column].astype(str).str.strip()
        elif dtype == "float":
            df[column] = df[column].astype(float)
        elif dtype == "int":
            df[column] = df[column].astype(int)
        else:
            raise ValueError(f"Tipo no soportado: {dtype}")
    return df


def _compare(spec_name: str, spec: dict) -> bool:
    expected_path = spec["expected"]
    actual_path = spec["actual"]

    print(f"==> {spec_name}")
    missing = [p for p in [expected_path, actual_path] if not p.exists()]
    if missing:
        for path in missing:
            print(f"Archivo no encontrado: {path}")
        return False

    df_expected = pd.read_csv(expected_path)
    df_actual = pd.read_csv(actual_path)

    df_expected = _normalize_columns(df_expected)[spec["columns"]]
    df_actual = _normalize_columns(df_actual)[spec["columns"]]

    print(f"Dataset Esperado: {len(df_expected)} filas.")
    print(f"Dataset Sistema Go: {len(df_actual)} filas.")

    df_expected = _apply_types(df_expected, spec["types"])
    df_actual = _apply_types(df_actual, spec["types"])

    df_expected = df_expected.sort_values(by=spec["sort"]).reset_index(drop=True)
    df_actual = df_actual.sort_values(by=spec["sort"]).reset_index(drop=True)

    try:
        pd.testing.assert_frame_equal(
            df_expected, df_actual, check_exact=False, atol=spec["atol"]
        )
        print("Output correcto.")
        return True
    except AssertionError as exc:
        print("Output incorrecto. Se encontraron diferencias:")
        print(exc)
        if len(df_expected) == len(df_actual):
            diff = df_expected.compare(df_actual)
            print(diff.head(10))
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verifica outputs contra los esperados."
    )

    parser.add_argument(
        "--expected-dir",
        type=Path,
        default=EXPECTED_DIR,
        help="Directorio con los archivos de resultados esperados.",
    )

    args = parser.parse_args()

    query_specs = get_queries_spec(args.expected_dir)

    all_ok = True
    for query_name in sorted(query_specs.keys()):
        ok = _compare(query_name, query_specs[query_name])
        all_ok = all_ok and ok
        print("")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
