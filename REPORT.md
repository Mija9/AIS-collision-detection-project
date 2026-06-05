# Report: Detection of Vessel Collisions in Danish AIS Data

## Objective

The objective was to identify the two vessels with the closest likely physical encounter, interpreted as a collision or near-collision, inside a 50 nautical mile search radius around latitude `55.225000`, longitude `14.245000`, during the period from `2021-12-01 00:00:00 UTC` to before `2022-01-01 00:00:00 UTC`.

## Data and tools

The solution uses Python 3 and Apache Spark through PySpark. Spark is used for loading, filtering, window functions, joins, aggregation, and writing outputs. Pandas is used only after the final event is selected, when the 20-minute trajectory window is small enough to plot locally.

The input data are Danish AIS CSV files for December 2021. The official AIS source provides historical AIS data as monthly zip files containing CSV records.

## Methodology

### 1. Loading and normalisation

The loader reads CSV files recursively from `data/`, renames the Danish AIS columns into stable snake_case column names, parses timestamps, and casts MMSI, latitude, longitude, SOG, COG, and heading into numeric types.

The processing window is implemented as an inclusive start and exclusive end:

```text
2021-12-01 00:00:00 <= timestamp < 2022-01-01 00:00:00
```

### 2. Spatial filtering

The assignment area is a 50 nautical mile radius around the given centre coordinate. This is converted to kilometres using:

```text
50 nm * 1.852 = 92.6 km
```

For efficiency, the code applies a latitude/longitude bounding box before calculating exact Haversine distance to the centre. Only records within the exact 92.6 km radius are kept.

### 3. Data cleaning and noise handling

AIS data can contain duplicated records, invalid coordinates, missing timestamps, unrealistic speed values, and sudden GPS jumps. The cleaning step therefore removes:

- records with missing MMSI, timestamp, latitude, or longitude;
- latitude/longitude values outside valid world bounds;
- duplicate positions for the same vessel and timestamp;
- reported SOG values outside a realistic range;
- points whose implied speed from the previous point exceeds `75 knots`;
- records whose navigational status indicates anchored, moored, aground, or undefined;
- almost stationary vessel points, unless the vessel also shows meaningful displacement between consecutive positions.

This combination is designed to prevent two docked or anchored vessels from being treated as a collision and to avoid false positives caused by a single bad GPS jump.

### 4. Candidate generation

The main computational challenge is avoiding a full Cartesian product of all AIS records. The solution projects coordinates into a local kilometre grid around the assignment centre, then creates:

- a 60-second time bucket;
- an x/y spatial cell with cell size of 125 metres.

Each point is compared only with points from other vessels in the same or adjacent time bucket and spatial cell. After this limited join, exact Haversine separation is calculated. Candidate pairs are retained only when:

- the two MMSI values differ;
- the AIS timestamps are within `45 seconds`;
- the exact vessel separation is at most `80 metres`.

The top candidates are ranked by separation, then by timestamp difference.

### 5. Validation of the selected event

A close pair is not accepted immediately. The validator checks that both vessels have AIS records in the 20-minute trajectory window around the candidate event. The selected candidate must have enough points before and after the event for both vessels. This reduces the risk that the selected event is only a single isolated AIS anomaly.

### 6. Visualisation

For the final validated event, the code extracts each vessel's trajectory from exactly 10 minutes before to 10 minutes after the encounter timestamp. It saves:

- a CSV folder with trajectory records;
- a PNG plot showing both vessel paths and the closest encounter point;
- a JSON file summarising the MMSI numbers, vessel names, timestamp, coordinates, distance, and support counts.

## Findings

Run the Docker container on the December 2021 AIS CSV files and copy the values from `outputs/selected_event.json` here:

```text
Vessel A MMSI: <fill after run>
Vessel A name: <fill after run>
Vessel B MMSI: <fill after run>
Vessel B name: <fill after run>
Collision / closest encounter timestamp UTC: <fill after run>
Latitude: <fill after run>
Longitude: <fill after run>
Separation in metres: <fill after run>
AIS timestamp gap in seconds: <fill after run>
```

## Computational strategy

The expensive comparison is reduced by spatiotemporal indexing before exact distance calculation. The grid join dramatically reduces the number of pairs compared, because each AIS point is only matched against nearby cells and nearby time buckets. Spark handles this as distributed transformations, while the final `.toPandas()` conversion is used only for the small, already-selected trajectory window.

## Limitations

AIS is not a physical collision sensor. This approach detects the closest AIS-reported physical proximity between moving vessels. The result should therefore be interpreted as a collision candidate or closest-encounter event unless supported by external incident reports. The method also depends on AIS reporting frequency; a true collision may be missed if one vessel temporarily stops transmitting or reports delayed/incorrect positions.

## Findings

The validated closest vessel encounter was detected between:

- Vessel A: MMSI `265016790`, `RESCUE PATRIK DAHL`
- Vessel B: MMSI `265714460`, `RESCUE FAMOUS`
- Timestamp: `2021-12-13 17:48:52 UTC`
- Collision / closest encounter coordinates: `55.620884, 12.985381`
- Minimum separation: `0.29 meters`
- AIS timestamp difference between the two records: `16 seconds`

The trajectory validation found sufficient AIS support around the event. Vessel A had 19 trajectory points in the 20-minute window, with 9 before and 10 after the event. Vessel B had 428 trajectory points, with 252 before and 175 after the event.

The trajectory visualization is saved as:

`outputs/selected_collision_trajectory.png`