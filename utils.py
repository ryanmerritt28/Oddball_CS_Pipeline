import os
import pandas as pd

DEFAULT_ACTIONS = {"add", "update", "delete"}


def make_output_dir(path: str):
    """
    Create output dir based on user input
    """
    os.makedirs(path, exist_ok=True)


def get_month_from_filename(path: str):
    """
    Extracting YYYYMM from filename
    """

    fname = os.path.basename(path)                                  # e.g. "agents_202502.csv"
    base, _ = os.path.splitext(fname)                               # -> ("agents_202502", ".csv")
    parts = base.split("_")                                         # -> ["agents", "202503"]
    if parts and parts[-1].isdigit() and len(parts[-1]) == 6:
        return parts[-1]
    return None


def read_table(path: str):
    """
    Reads tables based on file type. Should be .csv for initial and delta. Can be others for final reports, which are
    used to generate the Output Report
    """

    filename, ext = os.path.splitext(path)
    if ext == ".csv":
        return pd.read_csv(path)
    elif ext in (".json",):
        return pd.read_json(path, lines=False)
    elif ext in ("parquet",):
        return pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported format: {ext}")


def write_table(df: pd.DataFrame, path: str, fmt: str = "csv"):
    """
    Writes table to filetype based on user input
    """

    if fmt == "csv":
        df.to_csv(path, index=False)
    elif fmt == "json":
        df.to_json(path, orient="records", indent=2)
    elif fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def apply_delta(base_df: pd.DataFrame, delta_df: pd.DataFrame, id_col: str):
    """
    Applies the delta functions (add, update, delete) to the initial file
    """

    # Delta file missing action column
    if "action" not in delta_df.columns:
        raise ValueError("Delta file must include an 'action' column")

    delta = delta_df.copy()

    # Normalize actions to all lower-case and no-spaces
    delta["action"] = delta["action"].astype(str).str.lower().str.strip()

    # Covering for invalid/unknown actions present in the delta file
    invalid_actions = set(delta['action'].unique()) - DEFAULT_ACTIONS
    if invalid_actions:
        raise ValueError(f"Invalid action(s) found: {invalid_actions}")

    # Split by action
    adds = delta[delta["action"] == "add"].drop(columns=["action"])
    updates = delta[delta["action"] == "update"].drop(columns=["action"])
    deletes = delta[delta["action"] == "delete"][id_col].unique().tolist()

    out = base_df.copy()

    # Delete
    if deletes:
        out = out[~out[id_col].isin(deletes)]

    # Update
    if not updates.empty:
        # drop rows being updated, append new data
        rows_not_updated = out[~out[id_col].isin(updates[id_col])]
        out = pd.concat([rows_not_updated, updates], ignore_index=True).fillna('Unknown')

    # Add
    if not adds.empty:
        # treat as update if ID exists
        rows_not_added = out[~out[id_col].isin(adds[id_col])]
        out = pd.concat([rows_not_added, adds], ignore_index=True).fillna('Unknown')

    out = out.reset_index(drop=True)

    return out



