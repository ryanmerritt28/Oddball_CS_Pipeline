import os
import pandas as pd

from utils import read_table


def load_report(report_path: str):
    """
    Load output report from step 2
    """
    return read_table(report_path)


def q1_total_interactions_by_center(report: pd.DataFrame):
    """
    What were the total number of interactions handled by each contact center in Q1 2025?
    Group report by contact center, sum total interactions
    """
    result = (
        report.groupby("contact_center_name")["total_interactions"]
        .sum()
        .reset_index()
    )
    return result


def q2_month_highest_interactions(report: pd.DataFrame):
    """
    Which month (Jan, Feb, or Mar) had the highest total interaction volume?
    Group by month and sum of total interactions. Sort highest->lowest and report first result
    """
    result = (
        report.groupby("month")["total_interactions"]
        .sum()
        .reset_index()
        .sort_values("total_interactions", ascending=False)
        .head(1)
    )

    return result


def q3_longest_avg_call(report: pd.DataFrame):
    """
    Which contact center had the longest average phone call duration (total_call_duration)?
        Why might this be the case based on the interactions data?
        What approach would you recommend to measure agent work time more accurately?

    Calculate average call duration, then group by contact center name
    """
    report = report.copy()
    report = report.groupby(
        "contact_center_name"
    ).agg(
        total_call_duration=("total_call_duration", "sum"),
        total_calls=("total_calls", "sum")
    )

    report["avg_call_duration"] = report["total_call_duration"] / report["total_calls"]

    result = (
        report.sort_values("avg_call_duration", ascending=False).head(1)
    )

    return result


def run_answers(report_path: str):
    """Run all business questions and print answers."""
    report = load_report(report_path)

    print("\nQ1: Total number of interactions handled by each contact center in Q1 2025")
    print(q1_total_interactions_by_center(report))

    print("\nQ2: Which month had the highest total interaction volume?")
    print(q2_month_highest_interactions(report))

    print("\nQ3: Which contact center had the longest average phone call duration?")
    print(q3_longest_avg_call(report))

    print("\nDiscussion:")
    print("- The Boston MA contact center has a number of outliers on the high end of call duration.")
    print("- Most of these have a 5-minute delay between the agent resolution timestamp and the end of the interaction")
    print("- To measure agent work time more accurately, we should use the agent resolution timestamp to calculate the"
          "duration of the call")


if __name__ == "__main__":
    rep_path = os.path.join("report", "support_report.csv")
    run_answers(rep_path)

