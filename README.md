# Big Data Examination: Detection of Vessel Collisions

This repository contains a Dockerised PySpark pipeline for finding the closest physical vessel encounter in the Danish AIS dataset during December 2021 inside a 50 nautical mile radius around latitude `55.225000`, longitude `14.245000`.

The code is intentionally organised as a small data-engineering pipeline:

1. load and normalise Danish AIS CSV files;
2. restrict the records to December 2021;
3. remove invalid coordinates, points outside the assignment area, stationary vessels, and GPS jumps;
4. use spatiotemporal bucketing to avoid a full vessel-to-vessel Cartesian join;
5. rank close encounters by distance and AIS timestamp difference;
6. validate the best candidates by checking that both vessels have trajectory support before and after the encounter;
7. save the final event summary and a 20-minute trajectory plot.

## Data

Download the Danish AIS files for December 2021 from the official historical AIS source and extract the CSV files into `data/`. The Danish AIS page states that historical AIS data are free to download and are provided as monthly zip files containing CSV data.

Expected layout:

```text
data/
  aisdk-2021-12-01.csv
  aisdk-2021-12-02.csv
  ...
```

The Spark reader is recursive, so subdirectories are also fine.

## Run with Docker

Build the image:

```bash
docker build -t mija-ais-collision .
```

Run it, mounting local data and outputs:

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/outputs:/app/outputs" \
  mija-ais-collision
```

Alternative with Compose:

```bash
docker compose up --build
```

## Run locally without Docker

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.main
```

## Main outputs

After a successful run, the `outputs/` folder contains:

- `ranked_candidate_encounters/`: Spark CSV folder with the closest candidate pairs before final validation;
- `selected_event.json`: MMSI numbers, vessel names, UTC timestamp, coordinates, separation, and support counts;
- `selected_trajectory_csv/`: Spark CSV folder with both vessel trajectories from 10 minutes before to 10 minutes after the event;
- `selected_collision_trajectory.png`: final map-like trajectory visualisation.

## Why this is efficient

The expensive step is comparing vessels to each other. This implementation does not compare every AIS point with every other AIS point. Instead, it projects latitude/longitude into a local kilometre grid near Bornholm, assigns each record to a time bucket and spatial cell, and only joins records in the same or neighbouring cells and time buckets. Exact Haversine distance is calculated only after this pruning step.

## Important parameters

The most relevant settings are in `src/settings.py`:

- `COLLISION_RADIUS_METERS = 80.0`
- `TIME_TOLERANCE_SECONDS = 45`
- `GRID_SIZE_METERS = 125.0`
- `MIN_MOVING_SOG_KNOTS = 0.6`
- `MAX_IMPLIED_SOG_KNOTS = 75.0`

These values are documented in `REPORT.md`. If the first run produces no validated event, inspect `outputs/ranked_candidate_encounters/` and adjust the validation thresholds carefully rather than immediately increasing the collision radius.
