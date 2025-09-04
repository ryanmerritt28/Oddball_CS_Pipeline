import argparse
import glob
import os
import pandas as pd

from utils import (
    make_output_dir,
    get_month_from_filename,
    read_table,
    write_table,
    apply_delta
)


def list_initial_files(data_dir: str):
    """
    Return the paths for the initial files
    """
    initial_dir = os.path.join(data_dir, "initial")

    files = {
        "agents": os.path.join(initial_dir, "agents.csv"),
        "contact_centers": os.path.join(initial_dir, "contact_centers.csv"),
        "service_categories": os.path.join(initial_dir, "service_categories.csv"),
        "interactions": os.path.join(initial_dir, "interactions.csv"),
    }

    return files


def list_delta_files(data_dir: str):
    """
    Return the paths for the delta files grouped by type (agents, interactions, etc.)
    """
    delta_dir = os.path.join(data_dir, "delta")
    patterns = {
        "agents": os.path.join(delta_dir, "agents_*.csv"),
        "contact_centers": os.path.join(delta_dir, "contact_centers_*.csv"),
        "service_categories": os.path.join(delta_dir, "service_categories_*.csv"),
        "interactions": os.path.join(delta_dir, "interactions_*.csv"),
    }

    # Collect matching files into a dictionary
    out = {}
    for table_name, pattern in patterns.items():
        files = sorted(glob.glob(pattern))
        out[table_name] = files

    return out


def convert_utc_to_est(ts_series: pd.Series):
    """
    Converts datetime series from UTC to EST
    """
    return pd.to_datetime(ts_series, utc=True).dt.tz_convert("US/Eastern")


def handle_missing(interactions, agents, contact_centers, service_categories):
    """
    Ensures that deleted ID fields are replaced with "Unknown" in the final Interactions file.
    """
    final_agents = set(agents["agent_id"].unique())
    final_contact_centers = set(contact_centers["contact_center_id"].unique())
    final_service_categories = set(service_categories["category_id"].unique())

    interactions.loc[~interactions["agent_id"].isin(final_agents), "agent_id"] = "Unknown"
    interactions.loc[~interactions["contact_center_id"].isin(final_contact_centers), "contact_center_id"] = "Unknown"
    interactions.loc[~interactions["category_id"].isin(final_service_categories), "category_id"] = "Unknown"

    return interactions


def process(args):
    """
    Running the pipeline. Loads initial and delta data, applies delta tables, fixes references, converts timezones,
    and saves final state
    """
    make_output_dir(args.out_dir)

    # ---------------------------
    # STEP 1: LOAD INITIAL DATA
    # ---------------------------

    initial_files = list_initial_files(args.data_dir)

    agents = read_table(initial_files["agents"])
    contact_centers = read_table(initial_files["contact_centers"])
    service_categories = read_table(initial_files["service_categories"])
    interactions = read_table(initial_files["interactions"])

    required_id_columns = [
        (agents, "agents", "agent_id"),
        (contact_centers, "contact_centers", "contact_center_id"),
        (service_categories, "service_categories", "category_id"),
        (interactions, "interactions", "interaction_id")
    ]

    for df, name, id_col in required_id_columns:
        if id_col not in df.columns:
            raise ValueError(f"{name} table is missing the required ID column '{id_col}'")

    # -------------------------------------------
    # STEP 2: APPLY DELTAS AND HANDLE MISSING IDs
    # -------------------------------------------

    delta_files = list_delta_files(args.data_dir)

    # Filter by months, if provided
    months_filter = set()
    for m in args.months.split(","):
        cleaned = m.strip()
        if cleaned:
            months_filter.add(cleaned)

    def iterate_deltas(table_type: str):
        """
        Filters for delta files based on the provided YYYYMM codes
        """
        result = []

        files = delta_files[table_type]
        for file_path in files:
            month_code = get_month_from_filename(file_path)

            if months_filter and month_code not in months_filter:
                continue

            result.append((month_code, file_path))

        return result

    # Grabbing selected delta files and applying the add/update/delete actions to the initial file
    for month, path in iterate_deltas("agents"):
        delta_data = read_table(path)
        agents = apply_delta(agents, delta_data, "agent_id")

    for month, path in iterate_deltas("contact_centers"):
        delta_data = read_table(path)
        contact_centers = apply_delta(contact_centers, delta_data, "contact_center_id")

    for month, path in iterate_deltas("service_categories"):
        delta_data = read_table(path)
        service_categories = apply_delta(service_categories, delta_data, "category_id")

    for month, path in iterate_deltas("interactions"):
        delta_data = read_table(path)
        interactions = apply_delta(interactions, delta_data, "interaction_id")

    # Fixes deleted IDs if they were deleted in a delta file
    interactions = handle_missing(interactions, agents, contact_centers, service_categories)

    # ---------------------------
    # STEP 3: CONVERT TIMESTAMPS
    # ---------------------------

    if "timestamp" in interactions.columns:
        try:
            interactions["timestamp"] = convert_utc_to_est(interactions["timestamp"])
        except Exception as e:
            print(f"Could not convert timestamps: {e}")

    if "interaction_start" in interactions.columns:
        try:
            interactions["interaction_start"] = convert_utc_to_est(interactions["interaction_start"])
        except Exception as e:
            print(f"Could not convert timestamps: {e}")

    if "agent_resolution_timestamp" in interactions.columns:
        try:
            interactions["agent_resolution_timestamp"] = convert_utc_to_est(interactions["agent_resolution_timestamp"])
        except Exception as e:
            print(f"Could not convert timestamps: {e}")

    if "interaction_end" in interactions.columns:
        try:
            interactions["interaction_end"] = convert_utc_to_est(interactions["interaction_end"])
        except Exception as e:
            print(f"Could not convert timestamps: {e}")

    # -------------------------------
    # STEP 4: WRITE FINAL DATA TABLES
    # -------------------------------
    def save_table(df: pd.DataFrame, name: str):
        """
        Writes tables based on desired format, with the filename set to [table_name]_final.[ext]
        """
        output_file = os.path.join(args.out_dir, f"{name}_final.{args.format}")
        write_table(df, output_file, fmt=args.format)

    save_table(agents, "agents")
    save_table(contact_centers, "contact_centers")
    save_table(service_categories, "service_categories")
    save_table(interactions, "interactions")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the data pipeline.")
    parser.add_argument("--data-dir", default="./data", help="Path to folder containing initial/ and delta/")
    parser.add_argument("--out-dir", default="./output", help="Path to write outputs to.")
    parser.add_argument("--format", default="csv", choices=["csv", "json", "parquet"],
                        help="Output format. Can be 'csv', 'json', or 'parquet'.")
    parser.add_argument("--months", default="202502, 202503", help="Optional months to process, e.g. 202502, 202503")
    args = parser.parse_args()

    process(args)
