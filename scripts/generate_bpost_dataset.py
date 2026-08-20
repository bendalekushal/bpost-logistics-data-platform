#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import shutil
from pathlib import Path
from typing import Dict, Sequence

import numpy as np
import pandas as pd
from faker import Faker


SEED = 42
DEFAULT_OUTPUT = Path("./data_lake_raw")

COUNTS: Dict[str, int] = {
    "dim_vehicle": 5_000,
    "dim_driver": 3_000,
    "dim_facility_hub": 25,
    "dim_route_template": 300,

    "fact_telematics_raw": 150_000,
    "fact_vehicle_diagnostics_stream": 125_000,
    "fact_driver_behavior_event": 75_000,
    "fact_trip_tour_execution": 25_000,
    "fact_stop_arrival_departure": 50_000,
    "bridge_trip_consignment": 40_000,
    "fact_fuel_charging_transaction": 25_000,
    "fact_maintenance_work_order": 10_000,
    "fact_tachograph_compliance": 20_000,

    "dim_parcel": 25_000,
    "dim_sorting_machine": 100,
    "dim_sort_chute": 700,
    "dim_sort_plan_matrix": 300,
    "dim_missort_reason_code": 15,

    "fact_sorter_scan_event": 200_000,
    "fact_camera_vision_diagnostics": 100_000,
    "fact_parcel_missort_incident": 10_000,
    "fact_manual_rework_action": 8_000,
    "fact_container_manifest_audit": 25_000,

    "agg_daily_missort_kpi": 2_000,
}

FACILITIES = [
    ("HUB-BRU-X", "Brussels X", "1000", "HUB", 50.8503, 4.3517),
    ("HUB-ANT-X", "Antwerp X", "2000", "HUB", 51.2194, 4.4025),
    ("HUB-GHT-X", "Ghent X", "9000", "HUB", 51.0543, 3.7174),
    ("HUB-LIE-X", "Liege X", "4000", "HUB", 50.6326, 5.5797),
    ("HUB-CHA-X", "Charleroi X", "6000", "HUB", 50.4108, 4.4446),
    ("PDC-BRU-1000", "Brussels Delivery Centre", "1000", "PDC", 50.8467, 4.3525),
    ("PDC-ANT-2000", "Antwerp Delivery Centre", "2000", "PDC", 51.2142, 4.4147),
    ("PDC-GHT-9000", "Ghent Delivery Centre", "9000", "PDC", 51.0472, 3.7253),
    ("PDC-LIE-4000", "Liege Delivery Centre", "4000", "PDC", 50.6404, 5.5731),
    ("PDC-CHA-6000", "Charleroi Delivery Centre", "6000", "PDC", 50.4214, 4.4483),
    ("HUB-MEC-X", "Mechelen X", "2800", "HUB", 51.0259, 4.4776),
    ("HUB-HAS-X", "Hasselt X", "3500", "HUB", 50.9307, 5.3325),
    ("PDC-NAM-5000", "Namur Delivery Centre", "5000", "PDC", 50.4674, 4.8718),
    ("PDC-MON-7000", "Mons Delivery Centre", "7000", "PDC", 50.4542, 3.9523),
    ("PDC-KOR-8500", "Kortrijk Delivery Centre", "8500", "PDC", 50.8280, 3.2640),
    ("PDC-BRU-S", "Brussels South Delivery Centre", "1180", "PDC", 50.8010, 4.3374),
    ("PDC-LEU-3000", "Leuven Delivery Centre", "3000", "PDC", 50.8798, 4.7005),
    ("PDC-TUR-2300", "Turnhout Delivery Centre", "2300", "PDC", 51.3225, 4.9440),
    ("PDC-ROE-8800", "Roeselare Delivery Centre", "8800", "PDC", 50.9446, 3.1246),
    ("PDC-OOS-8400", "Ostend Delivery Centre", "8400", "PDC", 51.2301, 2.9192),
    ("PDC-HER-2200", "Herentals Delivery Centre", "2200", "PDC", 51.1767, 4.8325),
    ("PDC-VER-1800", "Vilvoorde Delivery Centre", "1800", "PDC", 50.9280, 4.4285),
    ("PDC-WAV-1300", "Wavre Delivery Centre", "1300", "PDC", 50.7164, 4.6010),
    ("PDC-ARL-6700", "Arlon Delivery Centre", "6700", "PDC", 49.6833, 5.8167),
    ("PDC-BRU-W", "Brussels West Delivery Centre", "1080", "PDC", 50.8520, 4.3220),
]

SERVICE_TYPES = ["BPACK", "B2C", "B2B", "EXPRESS", "SAME_DAY", "ECONOMY"]
VEHICLE_TYPES = ["VAN", "RIGID_TRUCK", "TRACTOR", "ELECTRIC_VAN"]
EMISSION_CLASSES = ["EURO_5", "EURO_6", "EURO_6D", "ELECTRIC"]
LICENSE_CLASSES = ["B", "C", "CE"]
EVENT_TYPES = [
    "HARSH_BRAKING",
    "HARSH_ACCELERATION",
    "OVERSPEED",
    "IDLING",
    "DISTRACTED_DRIVING",
]
MACHINE_MODELS = ["SORT-X200", "SORT-X500", "SORTER-PRO", "AUTO-SORT-9000"]
REWORK_ACTIONS = [
    "Relabel parcel",
    "Manual chute reassignment",
    "Barcode rescan",
    "Destination correction",
    "Container reassignment",
]
REASON_CODES = [
    ("DESTINATION", "Destination postal-code routing mismatch"),
    ("LABEL", "Unreadable or damaged parcel label"),
    ("MACHINE", "Sorter diversion or sensor anomaly"),
    ("WEIGHT", "Weight mismatch caused wrong routing"),
    ("MANIFEST", "Container or manifest discrepancy"),
    ("ADDRESS", "Address parsing or postal-code issue"),
    ("BARCODE", "Barcode unreadable or invalid"),
    ("CHUTE", "Incorrect chute configuration"),
    ("SORT_PLAN", "Outdated sorting plan"),
    ("SCAN", "Missing or duplicate scan event"),
    ("ADDRESS_FORMAT", "Invalid address format"),
    ("POSTCODE", "Invalid destination postal code"),
    ("CONTAINER", "Incorrect container assignment"),
    ("OPERATOR", "Manual processing error"),
    ("SYSTEM", "Sorting system processing failure"),
]

MESSY_MIN = 0.05
MESSY_MAX = 0.12
DUPLICATE_RATE = 0.02
ORPHAN_RATE = 0.03


def id_array(prefix: str, n: int, rng: np.random.Generator, width: int = 6) -> np.ndarray:
    values = rng.integers(1, 10**width, size=n)
    return np.array([f"{prefix}-{v:0{width}d}" for v in values], dtype=object)


def hex_ids(n: int, rng: np.random.Generator) -> np.ndarray:
    values = rng.integers(0, 2**63 - 1, size=n)
    return np.array(
        [hashlib.md5(f"{v}-{i}-{SEED}".encode()).hexdigest() for i, v in enumerate(values)],
        dtype=object,
    )


def timestamps(n: int, rng: np.random.Generator) -> pd.Series:
    start = pd.Timestamp("2026-01-01", tz="UTC").value
    end = pd.Timestamp("2026-08-20 23:59:59", tz="UTC").value
    values = rng.integers(start, end, size=n)
    return pd.Series(pd.to_datetime(values, utc=True))


def messy_timestamps(
    series: pd.Series,
    rng: np.random.Generator,
    rate: float = 0.08,
) -> np.ndarray:

    # Make an explicitly writable NumPy object array
    out = np.array(
        series.dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        dtype=object,
        copy=True,
    )

    n = len(out)
    count = max(1, int(n * rate))

    idx = rng.choice(
        np.arange(n),
        size=count,
        replace=False,
    )

    split = max(1, count // 4)

    a = idx[:split]
    b = idx[split:2 * split]
    c = idx[2 * split:3 * split]
    d = idx[3 * split:]

    # Slash-format timestamps
    out[a] = (
        series.iloc[a]
        .dt.strftime("%d/%m/%Y %H:%M:%S")
        .to_numpy()
    )

    # Unix epoch seconds
    out[b] = (
        series.iloc[b]
        .astype("int64")
        .floordiv(10**9)
        .astype(str)
        .to_numpy()
    )

    # Sentinel dates
    out[c] = rng.choice(
        ["1900-01-01", "9999-12-31"],
        size=len(c),
    )

    # Null-like values
    out[d] = rng.choice(
        ["", "NULL", "None", "N/A"],
        size=len(d),
    )

    return out


def add_missing_values(
    df: pd.DataFrame,
    columns: Sequence[str],
    rng: np.random.Generator,
    rate: float,
) -> None:

    for column in columns:
        if column not in df.columns:
            continue

        # We intentionally introduce mixed-type corruption.
        # Convert the column to object before inserting strings such as
        # NULL, N/A, None, or blank values.
        df[column] = df[column].astype("object")

        n = max(1, int(len(df) * rate))

        idx = rng.choice(
            df.index.to_numpy(),
            size=min(n, len(df)),
            replace=False,
        )

        df.loc[idx, column] = rng.choice(
            ["", "NULL", "None", "N/A", np.nan],
            size=len(idx),
        )

def add_casing_noise(
    df: pd.DataFrame,
    columns: Sequence[str],
    rng: np.random.Generator,
    rate: float,
) -> None:
    for column in columns:
        if column not in df.columns:
            continue

        n = max(1, int(len(df) * rate))
        idx = rng.choice(df.index.to_numpy(), size=min(n, len(df)), replace=False)

        values = df.loc[idx, column].astype(str)
        modes = rng.choice(["lower", "upper", "pad"], size=len(values))

        transformed = []
        for value, mode in zip(values, modes):
            if mode == "lower":
                transformed.append(value.lower())
            elif mode == "upper":
                transformed.append(value.upper())
            else:
                transformed.append(f"  {value} ")

        df.loc[idx, column] = transformed


def add_numeric_errors(
    df: pd.DataFrame,
    columns: Sequence[str],
    rng: np.random.Generator,
    rate: float,
) -> None:

    errors = [
        "ERR_SENSOR",
        "N/D",
        "420.5g",
        "-999",
        "250",
    ]

    for column in columns:
        if column not in df.columns:
            continue

        # Numeric columns must become object because we deliberately
        # inject malformed strings into the raw landing data.
        df[column] = df[column].astype("object")

        n = max(1, int(len(df) * rate))

        idx = rng.choice(
            df.index.to_numpy(),
            size=min(n, len(df)),
            replace=False,
        )

        df.loc[idx, column] = rng.choice(
            errors,
            size=len(idx),
        )


def add_orphans(
    df: pd.DataFrame,
    fk_columns: Dict[str, str],
    rng: np.random.Generator,
    rate: float = ORPHAN_RATE,
) -> None:
    for column, prefix in fk_columns.items():
        if column not in df.columns:
            continue

        n = max(1, int(len(df) * rate))
        idx = rng.choice(df.index.to_numpy(), size=min(n, len(df)), replace=False)

        df.loc[idx, column] = [
            f"{prefix}-ORPHAN-{i:06d}"
            for i in range(len(idx))
        ]


def add_duplicates(
    df: pd.DataFrame,
    rng: np.random.Generator,
    rate: float = DUPLICATE_RATE,
) -> pd.DataFrame:
    n = max(1, int(len(df) * rate))
    sample = df.sample(
        n=min(n, len(df)),
        random_state=int(rng.integers(1, 2**31 - 1)),
    )
    return pd.concat([df, sample], ignore_index=True)


def belgian_postcodes(n: int, rng: np.random.Generator) -> np.ndarray:
    """
    Generate realistic Belgian postal codes.

    We first choose a Belgian postal-code region for each record,
    then generate one postcode inside that region.

    This implementation is fully vectorized and avoids constructing
    a large (number_of_regions x n) intermediate matrix.
    """

    low = np.array([
        1000, 2000, 3000, 4000, 5000,
        6000, 7000, 8000, 9000
    ])

    high = np.array([
        1299, 2999, 3999, 4999, 5999,
        6999, 7999, 8999, 9999
    ])

    # Select a region for every row.
    region_idx = rng.integers(
        0,
        len(low),
        size=n
    )

    # Generate one postcode inside the selected region.
    postcodes = rng.integers(
        low[region_idx],
        high[region_idx] + 1
    )

    return postcodes.astype(str)

def generate_dim_sort_plan_matrix(ctx: DatasetContext, n: int) -> pd.DataFrame:

    start = ctx.rng.integers(1000, 9999, n)
    span = ctx.rng.integers(5, 100, n)
    end = np.minimum(start + span, 9999)

    df = pd.DataFrame({
        "sort_plan_id": id_array("SORTPLAN-BE", n, ctx.rng, 5),
        "postal_code_range_start": start,
        "postal_code_range_end": end,
        "target_chute_id": ctx.rng.choice(
            ctx.chute_ids,
            n,
        ),
    })

    add_orphans(
        df,
        {
            "target_chute_id": "CHUTE-BE",
        },
        ctx.rng,
    )

    add_numeric_errors(
        df,
        [
            "postal_code_range_start",
            "postal_code_range_end",
        ],
        ctx.rng,
        0.05,
    )

    add_missing_values(
        df,
        [
            "postal_code_range_start",
            "postal_code_range_end",
        ],
        ctx.rng,
        0.06,
    )

    return add_duplicates(df, ctx.rng)


def parcel_barcodes(n: int, rng: np.random.Generator) -> np.ndarray:
    digits = rng.integers(0, 10, size=(n, 24))
    return np.array(["".join(row.astype(str)) for row in digits], dtype=object)


class DatasetContext:
    def __init__(self, rng: np.random.Generator, faker: Faker):
        self.rng = rng
        self.faker = faker

        self.facilities = pd.DataFrame(
            FACILITIES,
            columns=[
                "facility_id",
                "facility_name",
                "postal_code",
                "facility_type",
                "latitude",
                "longitude",
            ],
        )

        self.facility_ids = self.facilities["facility_id"].to_numpy()
        self.vehicle_ids = None
        self.driver_ids = None
        self.parcel_ids = None
        self.machine_ids = None
        self.chute_ids = None
        self.route_ids = None
        self.tour_ids = None
        self.reason_ids = None


def generate_dimensions(ctx: DatasetContext) -> Dict[str, pd.DataFrame]:

    n = COUNTS["dim_vehicle"]
    ctx.vehicle_ids = id_array("BEL-FL", n, ctx.rng, 5)

    vehicle = pd.DataFrame({
        "vehicle_id": ctx.vehicle_ids,
        "vin": np.array(
            [
                "VF1" + "".join(ctx.rng.choice(list("0123456789ABCDEF"), 14))
                for _ in range(n)
            ]
        ),
        "license_plate": [
            f"{ctx.rng.choice([1,2,3])}-"
            f"{''.join(ctx.rng.choice(list('ABCDEFGHJKLMNPRSTUVWXYZ'), 3))}-"
            f"{ctx.rng.integers(100,999)}"
            for _ in range(n)
        ],
        "fleet_category": ctx.rng.choice(VEHICLE_TYPES, n),
        "payload_capacity_kg": ctx.rng.choice(
            [750, 1200, 2500, 5000, 8500, 12000, 18000], n
        ),
        "emission_class": ctx.rng.choice(EMISSION_CLASSES, n),
        "depot_id": ctx.rng.choice(ctx.facility_ids, n),
    })

    add_missing_values(vehicle, ["fleet_category", "payload_capacity_kg", "emission_class"], ctx.rng, 0.06)
    add_casing_noise(vehicle, ["fleet_category", "emission_class"], ctx.rng, 0.06)
    add_numeric_errors(vehicle, ["payload_capacity_kg"], ctx.rng, 0.04)
    vehicle = add_duplicates(vehicle, ctx.rng)

    n = COUNTS["dim_driver"]
    ctx.driver_ids = id_array("DRV-EU", n, ctx.rng, 5)

    driver = pd.DataFrame({
        "driver_id": ctx.driver_ids,
        "driver_license_class": ctx.rng.choice(LICENSE_CLASSES, n),
        "adr_certified_flag": ctx.rng.choice([True, False], n, p=[0.25, 0.75]),
        "home_facility_id": ctx.rng.choice(ctx.facility_ids, n),
    })

    add_missing_values(driver, ["driver_license_class", "adr_certified_flag"], ctx.rng, 0.06)
    driver = add_duplicates(driver, ctx.rng)

    route_n = COUNTS["dim_route_template"]
    ctx.route_ids = id_array("ROUTE-BE", route_n, ctx.rng, 5)

    origin = ctx.rng.choice(ctx.facility_ids, route_n)
    destination = ctx.rng.choice(ctx.facility_ids, route_n)

    route = pd.DataFrame({
        "route_template_id": ctx.route_ids,
        "route_name": [f"{a} -> {b}" for a, b in zip(origin, destination)],
        "origin_facility_id": origin,
        "dest_facility_id": destination,
        "scheduled_distance_km": np.round(ctx.rng.uniform(5, 300, route_n), 1),
    })

    add_orphans(
        route,
        {
            "origin_facility_id": "FAC",
            "dest_facility_id": "FAC",
        },
        ctx.rng,
    )

    add_numeric_errors(route, ["scheduled_distance_km"], ctx.rng, 0.05)
    route = add_duplicates(route, ctx.rng)

    parcel_n = COUNTS["dim_parcel"]
    ctx.parcel_ids = id_array("PARCEL-BE", parcel_n, ctx.rng, 8)

    parcel = pd.DataFrame({
        "parcel_id": ctx.parcel_ids,
        "barcode": parcel_barcodes(parcel_n, ctx.rng),
        "service_type": ctx.rng.choice(SERVICE_TYPES, parcel_n),
        "origin_postcode": belgian_postcodes(parcel_n, ctx.rng),
        "dest_postcode": belgian_postcodes(parcel_n, ctx.rng),
        "sender_id": id_array("SENDER-BE", parcel_n, ctx.rng, 6),
    })

    add_missing_values(
        parcel,
        ["service_type", "origin_postcode", "dest_postcode", "sender_id"],
        ctx.rng,
        0.06,
    )
    add_casing_noise(parcel, ["service_type"], ctx.rng, 0.07)
    parcel = add_duplicates(parcel, ctx.rng)

    machine_n = COUNTS["dim_sorting_machine"]
    ctx.machine_ids = id_array("MACHINE-BE", machine_n, ctx.rng, 4)

    machine = pd.DataFrame({
        "machine_id": ctx.machine_ids,
        "facility_id": ctx.rng.choice(ctx.facility_ids, machine_n),
        "machine_model": ctx.rng.choice(
            ["SORT-X200", "SORT-X500", "SORTER-PRO", "AUTO-SORT-9000"],
            machine_n,
        ),
        "total_chutes": ctx.rng.choice([24, 32, 48, 64, 96], machine_n),
    })

    add_orphans(machine, {"facility_id": "FAC"}, ctx.rng)
    machine = add_duplicates(machine, ctx.rng)

    chute_n = COUNTS["dim_sort_chute"]
    ctx.chute_ids = id_array("CHUTE-BE", chute_n, ctx.rng, 5)

    chute = pd.DataFrame({
        "chute_id": ctx.chute_ids,
        "machine_id": ctx.rng.choice(ctx.machine_ids, chute_n),
        "chute_number": ctx.rng.integers(1, 97, chute_n),
        "assigned_destination_hub_id": ctx.rng.choice(ctx.facility_ids, chute_n),
    })
    sort_plan_n = COUNTS["dim_sort_plan_matrix"]

    sort_plan = generate_dim_sort_plan_matrix(
        ctx,
        sort_plan_n,
    )

    add_orphans(
        chute,
        {
            "machine_id": "MACHINE-BE",
            "assigned_destination_hub_id": "FAC",
        },
        ctx.rng,
    )
    add_numeric_errors(chute, ["chute_number"], ctx.rng, 0.04)
    chute = add_duplicates(chute, ctx.rng)

    reason_n = COUNTS["dim_missort_reason_code"]
    ctx.reason_ids = id_array("R", reason_n, ctx.rng, 3)

    reason_df = pd.DataFrame({
        "reason_code_id": ctx.reason_ids,
        "reason_category": [x[0] for x in REASON_CODES[:reason_n]],
        "description": [x[1] for x in REASON_CODES[:reason_n]],
    })

    return {
        "dim_vehicle": vehicle,
        "dim_driver": driver,
        "dim_facility_hub": ctx.facilities.copy(),
        "dim_route_template": route,
        "dim_parcel": parcel,
        "dim_sorting_machine": machine,
        "dim_sort_chute": chute,
        "dim_sort_plan_matrix": sort_plan,
        "dim_missort_reason_code": reason_df,
    }


def generate_fleet_facts(ctx: DatasetContext) -> Dict[str, pd.DataFrame]:

    n = COUNTS["fact_trip_tour_execution"]
    ctx.tour_ids = id_array("TOUR-BE", n, ctx.rng, 7)

    start = timestamps(n, ctx.rng)
    duration = ctx.rng.integers(30, 720, n)
    end = start + pd.to_timedelta(duration, unit="m")

    trip = pd.DataFrame({
        "tour_id": ctx.tour_ids,
        "route_template_id": ctx.rng.choice(ctx.route_ids, n),
        "vehicle_id": ctx.rng.choice(ctx.vehicle_ids, n),
        "driver_id": ctx.rng.choice(ctx.driver_ids, n),
        "start_time": messy_timestamps(start, ctx.rng),
        "end_time": messy_timestamps(end, ctx.rng),
        "actual_distance_km": np.round(ctx.rng.uniform(5, 350, n), 1),
        "fuel_consumed_liters_or_kwh": np.round(ctx.rng.uniform(2, 140, n), 2),
    })

    add_orphans(
        trip,
        {
            "route_template_id": "ROUTE-BE",
            "vehicle_id": "BEL-FL",
            "driver_id": "DRV-EU",
        },
        ctx.rng,
    )
    add_numeric_errors(
        trip,
        ["actual_distance_km", "fuel_consumed_liters_or_kwh"],
        ctx.rng,
        0.05,
    )
    trip = add_duplicates(trip, ctx.rng)

    n = COUNTS["fact_telematics_raw"]

    telematics = pd.DataFrame({
        "event_id": hex_ids(n, ctx.rng),
        "vehicle_id": ctx.rng.choice(ctx.vehicle_ids, n),
        "timestamp_utc": messy_timestamps(timestamps(n, ctx.rng), ctx.rng, 0.10),
        "latitude": np.round(ctx.rng.uniform(49.7, 51.5, n), 6),
        "longitude": np.round(ctx.rng.uniform(2.5, 6.3, n), 6),
        "speed_kmh": np.round(np.clip(ctx.rng.normal(58, 24, n), 0, 140), 1),
        "odometer_km": np.round(ctx.rng.uniform(1000, 900000, n), 2),
        "ignition_status": ctx.rng.choice(["ON", "OFF", "IDLE"], n),
    })

    add_orphans(telematics, {"vehicle_id": "BEL-FL"}, ctx.rng)
    add_numeric_errors(
        telematics,
        ["latitude", "longitude", "speed_kmh", "odometer_km"],
        ctx.rng,
        0.06,
    )
    add_missing_values(
        telematics,
        ["latitude", "longitude", "speed_kmh", "odometer_km"],
        ctx.rng,
        0.08,
    )
    telematics = add_duplicates(telematics, ctx.rng)

    n = COUNTS["fact_vehicle_diagnostics_stream"]

    diagnostics = pd.DataFrame({
        "diagnostic_id": hex_ids(n, ctx.rng),
        "vehicle_id": ctx.rng.choice(ctx.vehicle_ids, n),
        "timestamp_utc": messy_timestamps(timestamps(n, ctx.rng), ctx.rng, 0.10),
        "state_of_charge_pct": np.round(ctx.rng.uniform(10, 100, n), 1),
        "fuel_level_pct": np.round(ctx.rng.uniform(5, 100, n), 1),
        "dtc_fault_codes": ctx.rng.choice(
            ["", "P0420", "P0301", "P0171", "P2002", "C1234"],
            n,
            p=[0.70, 0.07, 0.06, 0.06, 0.05, 0.06],
        ),
        "tire_pressure_kpa": np.round(ctx.rng.normal(420, 28, n), 1),
    })

    add_orphans(diagnostics, {"vehicle_id": "BEL-FL"}, ctx.rng)
    add_numeric_errors(
        diagnostics,
        ["state_of_charge_pct", "fuel_level_pct", "tire_pressure_kpa"],
        ctx.rng,
        0.06,
    )
    diagnostics = add_duplicates(diagnostics, ctx.rng)

    n = COUNTS["fact_driver_behavior_event"]

    behavior = pd.DataFrame({
        "event_id": hex_ids(n, ctx.rng),
        "vehicle_id": ctx.rng.choice(ctx.vehicle_ids, n),
        "driver_id": ctx.rng.choice(ctx.driver_ids, n),
        "timestamp_utc": messy_timestamps(timestamps(n, ctx.rng), ctx.rng, 0.09),
        "event_type": ctx.rng.choice(EVENT_TYPES, n),
        "speed_kmh": np.round(np.clip(ctx.rng.normal(63, 22, n), 0, 150), 1),
    })

    add_orphans(
        behavior,
        {
            "vehicle_id": "BEL-FL",
            "driver_id": "DRV-EU",
        },
        ctx.rng,
    )
    add_numeric_errors(behavior, ["speed_kmh"], ctx.rng, 0.07)
    behavior = add_duplicates(behavior, ctx.rng)

    n = COUNTS["fact_stop_arrival_departure"]
    planned = timestamps(n, ctx.rng)
    arrival = planned + pd.to_timedelta(ctx.rng.integers(-20, 100, n), unit="m")
    departure = arrival + pd.to_timedelta(ctx.rng.integers(2, 90, n), unit="m")

    stop = pd.DataFrame({
        "stop_id": hex_ids(n, ctx.rng),
        "tour_id": ctx.rng.choice(ctx.tour_ids, n),
        "facility_id": ctx.rng.choice(ctx.facility_ids, n),
        "planned_eta": messy_timestamps(planned, ctx.rng),
        "actual_arrival_time": messy_timestamps(arrival, ctx.rng),
        "actual_departure_time": messy_timestamps(departure, ctx.rng),
        "dwell_time_seconds": (departure - arrival).dt.total_seconds().astype(int),
    })

    add_orphans(
        stop,
        {
            "tour_id": "TOUR-BE",
            "facility_id": "FAC",
        },
        ctx.rng,
    )
    add_numeric_errors(stop, ["dwell_time_seconds"], ctx.rng, 0.05)
    stop = add_duplicates(stop, ctx.rng)

    n = COUNTS["bridge_trip_consignment"]

    bridge = pd.DataFrame({
        "bridge_id": hex_ids(n, ctx.rng),
        "tour_id": ctx.rng.choice(ctx.tour_ids, n),
        "container_barcode": id_array("RC-BE", n, ctx.rng, 6),
        "sorting_center_origin_id": ctx.rng.choice(ctx.facility_ids, n),
        "sorting_center_dest_id": ctx.rng.choice(ctx.facility_ids, n),
    })

    add_orphans(
        bridge,
        {
            "tour_id": "TOUR-BE",
            "sorting_center_origin_id": "FAC",
            "sorting_center_dest_id": "FAC",
        },
        ctx.rng,
    )
    bridge = add_duplicates(bridge, ctx.rng)

    n = COUNTS["fact_fuel_charging_transaction"]

    energy = np.round(ctx.rng.uniform(5, 180, n), 2)
    fuel = pd.DataFrame({
        "transaction_id": hex_ids(n, ctx.rng),
        "vehicle_id": ctx.rng.choice(ctx.vehicle_ids, n),
        "facility_id": ctx.rng.choice(ctx.facility_ids, n),
        "timestamp_utc": messy_timestamps(timestamps(n, ctx.rng), ctx.rng, 0.09),
        "energy_amount": energy,
        "cost_eur": np.round(energy * ctx.rng.uniform(1.1, 2.3, n), 2),
    })

    add_orphans(
        fuel,
        {"vehicle_id": "BEL-FL", "facility_id": "FAC"},
        ctx.rng,
    )
    add_numeric_errors(fuel, ["energy_amount", "cost_eur"], ctx.rng, 0.06)
    fuel = add_duplicates(fuel, ctx.rng)

    n = COUNTS["fact_maintenance_work_order"]

    maintenance = pd.DataFrame({
        "work_order_id": id_array("WO-BE", n, ctx.rng, 7),
        "vehicle_id": ctx.rng.choice(ctx.vehicle_ids, n),
        "service_type": ctx.rng.choice(
            ["Brake", "Tyre", "Oil", "Engine", "Electrical", "Scheduled Service"],
            n,
        ),
        "cost_eur": np.round(ctx.rng.uniform(100, 8500, n), 2),
        "downtime_hours": np.round(ctx.rng.uniform(0.5, 72, n), 1),
    })

    add_orphans(maintenance, {"vehicle_id": "BEL-FL"}, ctx.rng)
    add_numeric_errors(maintenance, ["cost_eur", "downtime_hours"], ctx.rng, 0.06)
    maintenance = add_duplicates(maintenance, ctx.rng)

    n = COUNTS["fact_tachograph_compliance"]

    tachograph = pd.DataFrame({
        "compliance_log_id": hex_ids(n, ctx.rng),
        "driver_id": ctx.rng.choice(ctx.driver_ids, n),
        "vehicle_id": ctx.rng.choice(ctx.vehicle_ids, n),
        "shift_date": messy_timestamps(timestamps(n, ctx.rng), ctx.rng, 0.07),
        "driving_time_minutes": ctx.rng.integers(180, 720, n),
        "rest_break_minutes": ctx.rng.integers(15, 180, n),
        "infringement_type": ctx.rng.choice(
            ["NONE", "DRIVING_LIMIT", "REST_LIMIT", "BREAK_MISSED", "SPEED"],
            n,
        ),
    })

    add_orphans(
        tachograph,
        {"driver_id": "DRV-EU", "vehicle_id": "BEL-FL"},
        ctx.rng,
    )
    add_numeric_errors(
        tachograph,
        ["driving_time_minutes", "rest_break_minutes"],
        ctx.rng,
        0.05,
    )
    tachograph = add_duplicates(tachograph, ctx.rng)

    return {
        "fact_trip_tour_execution": trip,
        "fact_telematics_raw": telematics,
        "fact_vehicle_diagnostics_stream": diagnostics,
        "fact_driver_behavior_event": behavior,
        "fact_stop_arrival_departure": stop,
        "bridge_trip_consignment": bridge,
        "fact_fuel_charging_transaction": fuel,
        "fact_maintenance_work_order": maintenance,
        "fact_tachograph_compliance": tachograph,
    }


def generate_sorting_facts(ctx: DatasetContext) -> Dict[str, pd.DataFrame]:

    n = COUNTS["fact_sorter_scan_event"]

    intended = ctx.rng.choice(ctx.chute_ids, n)
    actual = intended.copy()

    mismatch = ctx.rng.random(n) < 0.08
    positions = np.flatnonzero(mismatch)

    if len(positions):
        actual[positions] = ctx.rng.choice(ctx.chute_ids, len(positions))

    scan_status = np.where(
        intended == actual,
        "SUCCESS",
        "MISSORT",
    )

    scan = pd.DataFrame({
        "scan_event_id": hex_ids(n, ctx.rng),
        "machine_id": ctx.rng.choice(ctx.machine_ids, n),
        "parcel_id": ctx.rng.choice(ctx.parcel_ids, n),
        "timestamp_utc": messy_timestamps(timestamps(n, ctx.rng), ctx.rng, 0.10),
        "measured_weight_g": np.round(
            np.clip(ctx.rng.normal(840, 380, n), 50, 25000),
            1,
        ),
        "intended_chute_id": intended,
        "actual_diverted_chute_id": actual,
        "sort_status": scan_status,
    })

    add_orphans(
        scan,
        {
            "machine_id": "MACHINE-BE",
            "parcel_id": "PARCEL-BE",
            "intended_chute_id": "CHUTE-BE",
            "actual_diverted_chute_id": "CHUTE-BE",
        },
        ctx.rng,
    )

    add_numeric_errors(scan, ["measured_weight_g"], ctx.rng, 0.07)
    scan = add_duplicates(scan, ctx.rng)

    n = COUNTS["fact_camera_vision_diagnostics"]

    vision = pd.DataFrame({
        "vision_event_id": hex_ids(n, ctx.rng),
        "scan_event_id": ctx.rng.choice(scan["scan_event_id"], n),
        "parcel_id": ctx.rng.choice(ctx.parcel_ids, n),
        "image_storage_path": [
            f"s3://bpost-vision-prod/2026/{ctx.rng.integers(1,13):02d}/{ctx.rng.integers(1,29):02d}/{hex_ids(1, ctx.rng)[0]}.jpg"
            for _ in range(n)
        ],
        "barcode_confidence_score": np.round(
            ctx.rng.uniform(0.35, 0.999, n),
            4,
        ),
        "label_damage_detected_flag": ctx.rng.choice(
            [True, False],
            n,
            p=[0.08, 0.92],
        ),
    })

    add_orphans(
        vision,
        {
            "scan_event_id": "SCAN",
            "parcel_id": "PARCEL-BE",
        },
        ctx.rng,
    )
    add_numeric_errors(vision, ["barcode_confidence_score"], ctx.rng, 0.05)
    vision = add_duplicates(vision, ctx.rng)

    n = COUNTS["fact_parcel_missort_incident"]

    expected = ctx.rng.choice(ctx.facility_ids, n)
    actual_facility = expected.copy()

    mismatch = ctx.rng.random(n) < 0.12
    positions = np.flatnonzero(mismatch)

    if len(positions):
        actual_facility[positions] = ctx.rng.choice(
            ctx.facility_ids,
            len(positions),
        )

    incident = pd.DataFrame({
        "incident_id": hex_ids(n, ctx.rng),
        "parcel_id": ctx.rng.choice(ctx.parcel_ids, n),
        "detected_timestamp": messy_timestamps(timestamps(n, ctx.rng), ctx.rng, 0.10),
        "detection_location_id": ctx.rng.choice(ctx.facility_ids, n),
        "expected_facility_id": expected,
        "actual_facility_id": actual_facility,
        "reason_code_id": ctx.rng.choice(ctx.reason_ids, n),
        "detection_stage": ctx.rng.choice(
            ["INDUCTION", "SORTING", "CONTAINERIZATION", "OUTBOUND_AUDIT"],
            n,
        ),
    })

    add_orphans(
        incident,
        {
            "parcel_id": "PARCEL-BE",
            "detection_location_id": "FAC",
            "expected_facility_id": "FAC",
            "actual_facility_id": "FAC",
            "reason_code_id": "R",
        },
        ctx.rng,
    )
    incident = add_duplicates(incident, ctx.rng)

    n = COUNTS["fact_manual_rework_action"]

    rework = pd.DataFrame({
        "rework_id": hex_ids(n, ctx.rng),
        "parcel_id": ctx.rng.choice(ctx.parcel_ids, n),
        "operator_id": id_array("OP-BE", n, ctx.rng, 6),
        "action_taken": ctx.rng.choice(REWORK_ACTIONS, n),
        "corrected_dest_postcode": belgian_postcodes(n, ctx.rng),
        "timestamp_out": messy_timestamps(timestamps(n, ctx.rng), ctx.rng, 0.09),
    })

    add_orphans(rework, {"parcel_id": "PARCEL-BE"}, ctx.rng)
    rework = add_duplicates(rework, ctx.rng)

    n = COUNTS["fact_container_manifest_audit"]

    audit = pd.DataFrame({
        "audit_id": hex_ids(n, ctx.rng),
        "container_id": id_array("RC-BE", n, ctx.rng, 6),
        "tour_id": ctx.rng.choice(ctx.tour_ids, n),
        "parcel_id": ctx.rng.choice(ctx.parcel_ids, n),
        "manifested_flag": ctx.rng.choice([True, False], n, p=[0.95, 0.05]),
        "discrepancy_type": ctx.rng.choice(
            ["NONE", "WRONG_CONTAINER", "MISSING_SCAN", "NOT_SCANNED"],
            n,
        ),
    })

    add_orphans(
        audit,
        {
            "tour_id": "TOUR-BE",
            "parcel_id": "PARCEL-BE",
        },
        ctx.rng,
    )
    audit = add_duplicates(audit, ctx.rng)

    n = COUNTS["agg_daily_missort_kpi"]

    kpi = pd.DataFrame({
        "kpi_id": id_array("KPI-BE", n, ctx.rng, 7),
        "sort_date": messy_timestamps(timestamps(n, ctx.rng), ctx.rng, 0.07),
        "facility_id": ctx.rng.choice(ctx.facility_ids, n),
        "machine_id": ctx.rng.choice(ctx.machine_ids, n),
        "shift_code": ctx.rng.choice(["A", "B", "C", "N"], n),
        "total_parcels_inducted": ctx.rng.integers(5000, 900000, n),
        "first_pass_sort_rate_pct": np.round(ctx.rng.uniform(93, 99.9, n), 3),
        "missort_rate_ppm": ctx.rng.integers(50, 5000, n),
    })

    add_orphans(
        kpi,
        {
            "facility_id": "FAC",
            "machine_id": "MACHINE-BE",
        },
        ctx.rng,
    )

    add_numeric_errors(
        kpi,
        [
            "total_parcels_inducted",
            "first_pass_sort_rate_pct",
            "missort_rate_ppm",
        ],
        ctx.rng,
        0.05,
    )

    return {
        "fact_sorter_scan_event": scan,
        "fact_camera_vision_diagnostics": vision,
        "fact_parcel_missort_incident": incident,
        "fact_manual_rework_action": rework,
        "fact_container_manifest_audit": audit,
        "agg_daily_missort_kpi": kpi,
    }


def write_all(output: Path, seed: int = SEED) -> None:

    if output.exists():
        shutil.rmtree(output)

    output.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed)

    faker = Faker("en_US")
    Faker.seed(seed)

    ctx = DatasetContext(rng, faker)

    dimensions = generate_dimensions(ctx)
    fleet_facts = generate_fleet_facts(ctx)
    sorting_facts = generate_sorting_facts(ctx)

    all_tables = {
        **dimensions,
        **fleet_facts,
        **sorting_facts,
    }

    manifest = []

    for table_name, df in all_tables.items():

        path = output / f"{table_name}.csv"

        df.to_csv(
            path,
            index=False,
            encoding="utf-8",
        )

        manifest.append({
            "table_name": table_name,
            "rows": len(df),
            "columns": len(df.columns),
            "size_mb": round(path.stat().st_size / (1024 ** 2), 3),
            "file": path.name,
        })

        print(
            f"{table_name:40s} "
            f"{len(df):>8,} rows "
            f"{path.stat().st_size / (1024 ** 2):>7.2f} MB"
        )

    manifest_df = pd.DataFrame(manifest)
    manifest_df.to_csv(
        output / "MANIFEST.csv",
        index=False,
    )

    total_rows = manifest_df["rows"].sum()
    total_size = manifest_df["size_mb"].sum()

    (output / "README.md").write_text(
        f"""# Enterprise Bpost Synthetic Dataset

Seed: {seed}

Tables: {len(all_tables)}

Rows: {total_rows:,}

CSV size: {total_size:.2f} MB

Injected characteristics:
- 5%-12% general data quality noise
- ~2% duplicate records
- ~3% orphan foreign keys
- mixed timestamp formats
- null-like strings
- casing / whitespace inconsistencies
- malformed numeric values
- intentional operational anomalies
""",
        encoding="utf-8",
    )

    print("\nGeneration complete")
    print(f"Rows      : {total_rows:,}")
    print(f"Size (MB) : {total_size:.2f}")
    print(f"Location  : {output.resolve()}")

    if not 40 <= total_size <= 90:
        print(
            "\nWARNING: CSV size is outside the requested 40-90 MB range. "
            "Adjust high-volume row counts if necessary."
        )


def main() -> None:

    parser = argparse.ArgumentParser(
        description="Generate enterprise synthetic Bpost logistics data."
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=SEED,
    )

    args = parser.parse_args()

    write_all(
        output=args.output,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()