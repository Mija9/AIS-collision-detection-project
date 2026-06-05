from datetime import timedelta
from pathlib import Path
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from src.settings import TRAJECTORY_WINDOW_MINUTES


def trajectory_window(moving_df: DataFrame, event: Row) -> DataFrame:
    start = event.event_timestamp - timedelta(minutes=TRAJECTORY_WINDOW_MINUTES)
    end = event.event_timestamp + timedelta(minutes=TRAJECTORY_WINDOW_MINUTES)
    return (
        moving_df.filter(F.col("mmsi").isin([event.mmsi_a, event.mmsi_b]))
        .filter((F.col("timestamp") >= F.lit(start)) & (F.col("timestamp") <= F.lit(end)))
        .select("mmsi", "vessel_name", "timestamp", "latitude", "longitude", "sog", "cog")
        .orderBy("mmsi", "timestamp")
    )


def write_outputs(track_df: DataFrame, event: Row, output_dir: Path) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    track_dir = output_dir / "selected_trajectory_csv"
    image_path = output_dir / "selected_collision_trajectory.png"
    json_path = output_dir / "selected_event.json"

    track_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(track_dir))
    pdf = track_df.toPandas()
    if pdf.empty:
        raise RuntimeError("The validated event has no trajectory points in the plotting window.")

    payload = {
        "mmsi_a": int(event.mmsi_a),
        "vessel_name_a": event.name_a or "UNKNOWN",
        "mmsi_b": int(event.mmsi_b),
        "vessel_name_b": event.name_b or "UNKNOWN",
        "timestamp_utc": str(event.event_timestamp),
        "latitude": float(event.event_latitude),
        "longitude": float(event.event_longitude),
        "distance_meters": float(event.separation_m),
        "time_gap_seconds": int(event.time_gap_seconds),
        "trajectory_points_a": int(event.points_a),
        "trajectory_points_b": int(event.points_b),
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    pdf["label"] = pdf.apply(lambda r: f"{int(r['mmsi'])} - {r['vessel_name'] or 'UNKNOWN'}", axis=1)
    fig, ax = plt.subplots(figsize=(10, 8))
    for label, group in pdf.groupby("label"):
        group = group.sort_values("timestamp")
        ax.plot(group["longitude"], group["latitude"], marker="o", markersize=3, linewidth=1.4, label=label)
        first = group.iloc[0]
        last = group.iloc[-1]
        ax.annotate("-10 min", (first["longitude"], first["latitude"]), fontsize=8)
        ax.annotate("+10 min", (last["longitude"], last["latitude"]), fontsize=8)

    ax.scatter([event.event_longitude], [event.event_latitude], marker="x", s=130, linewidths=2.5, label="closest encounter")
    ax.set_title(
        "AIS closest physical encounter near Bornholm\n"
        f"{event.event_timestamp} UTC | {event.separation_m:.2f} m | Δt {event.time_gap_seconds}s"
    )
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, alpha=0.35)
    ax.legend()
    fig.tight_layout()
    fig.savefig(image_path, dpi=220)
    plt.close(fig)
    return track_dir, image_path, json_path


def console_summary(event: Row) -> str:
    return "\n".join([
        "Validated closest encounter / possible collision:",
        f"  Vessel A: MMSI {event.mmsi_a}, name {event.name_a or 'UNKNOWN'}",
        f"  Vessel B: MMSI {event.mmsi_b}, name {event.name_b or 'UNKNOWN'}",
        f"  Timestamp: {event.event_timestamp} UTC",
        f"  Coordinates: {event.event_latitude:.6f}, {event.event_longitude:.6f}",
        f"  Separation: {event.separation_m:.2f} meters",
        f"  AIS time gap: {event.time_gap_seconds} seconds",
        f"  Support points: A={event.points_a} ({event.before_a} before, {event.after_a} after), "
        f"B={event.points_b} ({event.before_b} before, {event.after_b} after)",
    ])
