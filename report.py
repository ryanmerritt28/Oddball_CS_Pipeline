import os
import pandas as pd

from utils import read_table, write_table, make_output_dir


def build_report(data_dir: str, out_dir: str, fmt: str = "csv"):
    make_output_dir(out_dir)

    # Load final tables
    contact_centers = read_table(os.path.join(data_dir, f"contact_centers_final.{fmt}"))
    service_categories = read_table(os.path.join(data_dir, f"service_categories_final.{fmt}"))
    interactions = read_table(os.path.join(data_dir, f"interactions_final.{fmt}"))

    if "interaction_end" not in interactions.columns:
        raise ValueError("Interactions table must include an 'interaction_end' column.")

    # extract month from interaction date, pandas doesn't like datetimes with tz info, so I do this manually
    interactions["month"] = interactions["interaction_end"].astype(str).str[:7]

    # determine phone calls, set calls to value 1, others to 0
    interactions["is_call"] = interactions["channel"].str.lower().eq("phone").astype(int)

    # adding dimensions to interactions
    interactions = interactions.merge(
        contact_centers[["contact_center_id", "contact_center_name"]],
        on="contact_center_id",
        how="left"
    )

    interactions = interactions.merge(
        service_categories[["category_id", "department"]],
        on="category_id",
        how="left"
    )

    # group by month, concat center name, and department, calculate metrics
    grouped = interactions.groupby(
        ["month", "contact_center_name", "department"], dropna=False
    ).agg(
        total_interactions=("interaction_id", "count"),
        total_calls=("is_call", "sum"),
        total_call_duration=("call_duration_minutes", "sum")
    ).reset_index()

    # write report, defaulting table to export as .csv to
    report_path = os.path.join(out_dir, f"support_report.{fmt}")
    write_table(grouped, report_path, fmt=fmt)

    return grouped


# relative paths of final pipeline output and path to write report just hard-coded
build_report(data_dir='./output', out_dir='./report')
