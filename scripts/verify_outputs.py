import argparse
from pathlib import Path
import sys

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent.parent
EXPECTED_DIR = ROOT_DIR / "expected_outputs" / "expected_hi_medium"
OUTPUTS_DIR = ROOT_DIR / "outputs" / "client_0"

QUERY_SPECS = {
    "q1": {
        "expected": EXPECTED_DIR / "q1_results.csv",
        "actual": OUTPUTS_DIR / "q1_result.csv",
        "columns": ["account", "to_account", "amount"],
        "types": {"account": "str", "to_account": "str", "amount": "float"},
        "sort": ["account", "to_account", "amount"],
        "atol": 1e-2,
    },
    "q2": {
        "expected": EXPECTED_DIR / "q2_results.csv",
        "actual": OUTPUTS_DIR / "q2_result.csv",
        "columns": ["bank_name", "account", "amount"],
        "types": {"bank_name": "str", "account": "str", "amount": "float"},
        "sort": ["bank_name", "account", "amount"],
        "atol": 1e-2,
    },
    "q3": {
        "expected": EXPECTED_DIR / "q3_results.csv",
        "actual": OUTPUTS_DIR / "q3_result.csv",
        "columns": ["account", "amount"],
        "types": {"account": "str", "amount": "float"},
        "sort": ["account", "amount"],
        "atol": 1e-2,
    },
    "q4": {
        "expected": EXPECTED_DIR / "q4_results.csv",
        "actual": OUTPUTS_DIR / "q4_result.csv",
        "columns": ["bank", "account"],
        "types": {"bank": "str", "account": "str"},
        "sort": ["bank", "account"],
        "atol": 1e-2,
    },
    "q5": {
        "expected": EXPECTED_DIR / "q5_results.csv",
        "actual": OUTPUTS_DIR / "q5_result.csv",
        "columns": ["count"],
        "types": {"count": "int"},
        "sort": ["count"],
        "atol": 1e-2,
    },
}


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
        "--query",
        choices=sorted(QUERY_SPECS.keys()),
        action="append",
        help="Consulta especifica a verificar (ej: --query q1).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Ejecuta todas las verificaciones.",
    )
    args = parser.parse_args()

    if not args.all and not args.query:
        parser.error("Usa --all o al menos un --query.")

    queries = sorted(QUERY_SPECS.keys()) if args.all else args.query

    all_ok = True
    for query in queries:
        ok = _compare(query, QUERY_SPECS[query])
        all_ok = all_ok and ok
        print("")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
