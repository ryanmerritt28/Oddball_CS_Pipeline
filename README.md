# Customer Support Interactions Pipeline

This basic pipeline ingests data from an initial fact file, applies subsequent months' delta files, and writes the
results to a file type of the user's choice. We then use that final file to answer key business questions and draw
conclusions.

## Assumptions and Considerations
Initial fact files are loaded into project_root/data/initial, and delta files are loaded into project_root/data/initial. 
These can be modified with user input using --data-dir and specifying a directory containing the data.

Delta files should  include an "action" column that contains one of the following values: {add, update, delete}.

Timestamps are converted from UTC to EST and are labeled with the timezone information (-5:00 or -4:00 depending on the
status of daylight savings).

The pipeline will fill in null values from deleted dimension IDs or missing dimensions with "Unknown".

You should fully process the data for all available months before running the `report.py` or `answers.py` scripts


## Quick Start

Create a venv, install dependencies, and run the pipeline from the project root.

```
bash
python -m venv .venv && . .venv/bin/activate

windows
python -m venv venv
venv\Scripts\activate

(venv) cmd> pip install -r requirements.txt

1. Process the Data
(venv) cmd> python pipeline.py --data-dir ./data --out-dir ./output --format csv 

2. Output Report
(venv) cmd> python report.py

3. Answer Business Questions
(venv) cmd> python answers.py
```

## CLI Options
- `--data-dir`: optional, default `./data`, folder containing `initial/` and `delta/` 
- `--out-dir`: optional, default `./output`, folder to write the pipeline output file
- `--format`: optional, default `csv`, file format for output, e.g. `csv`, `json`, `parquet`
- `--months`: optional, default `202502, 202503`, specifies months to process if you only want to process Feb data

## Output
`./output` will contain:
- `agents_final.{ext}`
- `contact_centers_final.{ext}`
- `service_categories_final.{ext}`
- `interactions_final.{ext}`

`./report` will contain:
- `support_report.csv`

## Business Question Answers

See `answers.py` for code to answer these questions.

### 1. What were the total number of interactions handled by each contact center in Q1 2025?

`Atlanta GA SE: 8`
`Boston MA NE: 13`
`Richmond VA E: 7`

To get this answer, I `loaded the final interactions report, grouped by contact center name, and summed the total
interactions for each center`.

### 2. Which month (Jan, Feb, or Mar) had the highest total interaction volume?

`2025-02 (Feb): 10 interactions`

To get this answer, I `loaded the final interactions report, grouped by month and sum of total interactions.
I sorted the interactions from highest -> lowest and reported first result`

### 3. Which contact center had the longest average phone call duration (total_call_duration)?

`Boston MA NE` with an average call duration of `12.73 minutes`.

#### a. Why might this be the case based on the interaction data?

`The Boston MA contact center has a number of outliers on the high end of call duration. Most of these have a 
5-minute delay between the agent resolution timestamp and the end of the interaction`

#### b. What approach would you recommend to measure agent work time more accurately?

`To measure agent work time more accurately, we should use the agent resolution timestamp to calculate the
duration of the call`