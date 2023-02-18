import pandas as pd
import numpy as np
import os

# import datasets
df_alert = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw-data/aggregate_alerts_Kota Bandung.zip"),
    compression="zip",
)
df_irreg = pd.read_csv(
    os.path.join(
        os.getcwd(), "data/raw-data/aggregate_median_irregularities_Kota Bandung.zip"
    ),
    compression="zip",
)
df_jams = pd.read_csv(
    os.path.join(
        os.getcwd(), "data/raw-data/aggregate_median_jams_Kota Bandung_fixed.zip"
    ),
    compression="zip",
)

# filter dataset based on relevant columns
df_jams_filtered = df_jams[
    [
        "street",
        "time",
        "level",
        "median_length",
        "median_delay",
        "median_speed_kmh",
        "total_records",
        "geometry",
    ]
]

df_irreg_filtered = df_irreg[
    [
        "street",
        "time",
        "median_length",
        "median_seconds",
        "median_speed",
        "total_records",
        "geometry",
    ]
]

df_alert_filtered = df_alert[["street", "time", "type", "total_records"]]

# denormalization datasets into one dataset
df_merge = pd.merge(df_jams_filtered, df_irreg_filtered, on=["street", "time"])
df_merge = pd.merge(df_merge, df_alert_filtered, on=["street", "time"])

# rename columns
df_merge = df_merge.rename(
    {
        "street": "Street",
        "time": "Datetime",
        "level": "Jam Level",
        "median_length_x": "Jam Length (meters)",
        "median_delay": "Jam Time Spent (seconds)",
        "median_speed_kmh": "Jam Speed (Km/h)",
        "total_records_x": "Total Jam Records",
        "geometry_x": "Jam Geometry",
        "median_length_y": "Irregularities Length (meters)",
        "median_seconds": "Irregularities Time Spent (seconds)",
        "median_speed": "Irregularities Speed (Km/h)",
        "total_records_y": "Total Irregularities Records",
        "geometry_y": "Irregularities Geometry",
        "type": "Alert Type",
        "total_records": "Total Alert Records",
    },
    axis=1,
)

# convert Datetime column to DateTime type
df_merge["Datetime"] = pd.to_datetime(
    df_merge["Datetime"], format="%Y-%m-%d %H:%M:%S.%f"
)

# drop missing values
df_merge.dropna(inplace=True)

# make column Alert Type to lowercase
df_merge["Alert Type"] = df_merge["Alert Type"].str.title()

# export dataset to interim-data for further processing at the EDA stage
df_merge.to_csv(
    os.path.join(
        os.getcwd(), "data/interim-data/For EDA_Traffic Waze Kota Bandung.zip"
    ),
    compression="zip",
    index=False,
)
