from pathlib import Path

from pyspark import StorageLevel

from src.encounters import find_ranked_encounters
from src.load import load_assignment_month
from src.plotting import console_summary, trajectory_window, write_outputs
from src.preprocess import prepare_moving_area_points, keep_assignment_area, keep_valid_positions
from src.settings import CENTER_LAT, CENTER_LON, INPUT_GLOB, OUTPUT_DIR, SEARCH_RADIUS_KM, SEARCH_RADIUS_NM
from src.spark_app import make_spark
from src.validate import choose_supported_event


def main() -> None:
    output_dir = Path(OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = make_spark()
    try:
        print("Starting AIS collision search")
        print(f"Input path: {INPUT_GLOB}")
        print(f"Area: {SEARCH_RADIUS_NM:.0f} nm ({SEARCH_RADIUS_KM:.1f} km) around {CENTER_LAT}, {CENTER_LON}")

        raw = load_assignment_month(spark)
        print(f"Rows in December 2021: {raw.count():,}")
        print(f"MMSI count in month: {raw.select('mmsi').distinct().count():,}")

        valid = keep_valid_positions(raw)
        print(f"Rows with usable coordinates: {valid.count():,}")

        area = keep_assignment_area(valid).persist(StorageLevel.DISK_ONLY)
        print(f"Rows in assignment area: {area.count():,}")
        print(f"MMSI count in area: {area.select('mmsi').distinct().count():,}")

        moving = prepare_moving_area_points(raw).persist(StorageLevel.DISK_ONLY)
        print(f"Rows after motion/noise filters: {moving.count():,}")
        print(f"Moving MMSI count: {moving.select('mmsi').distinct().count():,}")

        candidates = find_ranked_encounters(moving).persist(StorageLevel.DISK_ONLY)
        n_candidates = candidates.count()
        candidate_dir = output_dir / "ranked_candidate_encounters"
        candidates.coalesce(1).write.mode("overwrite").option("header", True).csv(str(candidate_dir))
        print(f"Candidate encounters retained: {n_candidates:,}")
        print(f"Candidate table saved to: {candidate_dir}")

        event_df = choose_supported_event(candidates, moving)
        event = event_df.first()
        if event is None:
            print("No candidate passed the trajectory support checks. Try inspecting outputs/ranked_candidate_encounters.")
            return

        tracks = trajectory_window(moving, event)
        track_dir, image_path, json_path = write_outputs(tracks, event, output_dir)

        print(console_summary(event))
        print(f"Trajectory CSV saved to: {track_dir}")
        print(f"Trajectory plot saved to: {image_path}")
        print(f"Machine-readable event summary saved to: {json_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
