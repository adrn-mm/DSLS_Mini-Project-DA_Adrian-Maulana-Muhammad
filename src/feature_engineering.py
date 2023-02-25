import pandas as pd
import warnings
import os

warnings.filterwarnings("ignore")


# import datasets
df_alert_bandung = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw_data/aggregate_alerts_Kota Bandung.zip"),
    compression="zip",
    index_col=None,
)
df_alert_banjar = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw_data/aggregate_alerts_Kota Banjar.zip"),
    compression="zip",
    index_col=None,
)
df_alert_bekasi = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw_data/aggregate_alerts_Kota Bekasi.zip"),
    compression="zip",
    index_col=None,
)
df_alert_bogor = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw_data/aggregate_alerts_Kota Bogor.zip"),
    compression="zip",
    index_col=None,
)
df_alert_cimahi = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw_data/aggregate_alerts_Kota Cimahi.zip"),
    compression="zip",
    index_col=None,
)
df_alert_cirebon = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw_data/aggregate_alerts_Kota Cirebon.zip"),
    compression="zip",
    index_col=None,
)
df_alert_depok = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw_data/aggregate_alerts_Kota Depok.zip"),
    compression="zip",
    index_col=None,
)
df_alert_sukabumi = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw_data/aggregate_alerts_Kota Sukabumi.zip"),
    compression="zip",
    index_col=None,
)
df_alert_tasikmalaya = pd.read_csv(
    os.path.join(os.getcwd(), "data/raw_data/aggregate_alerts_Kota Tasikmalaya.zip"),
    compression="zip",
    index_col=None,
)

# feature engineering
lst_df = [
    df_alert_bandung,
    df_alert_banjar,
    df_alert_bekasi,
    df_alert_bogor,
    df_alert_cimahi,
    df_alert_cirebon,
    df_alert_depok,
    df_alert_sukabumi,
    df_alert_tasikmalaya,
]
lst_pivot_df = []

for df in lst_df:
    temp_df = df.copy()
    # select relevant column
    temp_df = temp_df[
        ["date", "kemendagri_kabupaten_nama", "street", "type", "total_records"]
    ]
    # pivot the df
    df_1 = temp_df[["date", "kemendagri_kabupaten_nama", "street"]]
    df_2 = temp_df.pivot(columns="type", values="total_records")
    df_pivot = pd.concat([df_1, df_2], axis=1)
    # drop na in categorical columns
    df_pivot.dropna(
        subset=["date", "kemendagri_kabupaten_nama", "street"], inplace=True
    )
    # fill na with 0 in numerical columns
    df_pivot.fillna(0, inplace=True)
    # convert numerical column to int64
    df_pivot.loc[:, "ACCIDENT":"WEATHERHAZARD"] = df_pivot.loc[
        :, "ACCIDENT":"WEATHERHAZARD"
    ].astype("Int64")
    # remove "KOTA" or "KABUPATEN"
    df_pivot["kemendagri_kabupaten_nama"] = (
        df_pivot["kemendagri_kabupaten_nama"]
        .str.replace("KOTA", "")
        .str.replace("KABUPATEN", "")
    )
    # Lowercase the city column
    df_pivot["kemendagri_kabupaten_nama"] = df_pivot[
        "kemendagri_kabupaten_nama"
    ].str.title()
    # remove space in city and street columns
    df_pivot["kemendagri_kabupaten_nama"] = df_pivot[
        "kemendagri_kabupaten_nama"
    ].str.strip()
    df_pivot["street"] = df_pivot["street"].str.strip()
    # reformat the date column
    df_pivot["date"] = pd.to_datetime(df_pivot["date"])
    df_pivot["date"] = df_pivot["date"].dt.strftime("%Y-%m-%d")
    # renaming columns
    df_pivot = df_pivot.rename(
        {
            "date": "Date",
            "kemendagri_kabupaten_nama": "City",
            "street": "Street",
            "ACCIDENT": "Accident",
            "JAM": "Jam",
            "ROAD_CLOSED": "Road Closed",
            "WEATHERHAZARD": "Weather Hazard",
        },
        axis=1,
    )
    lst_pivot_df.append(df_pivot)

# reassign the dataframes
(
    df_alert_bandung,
    df_alert_banjar,
    df_alert_bekasi,
    df_alert_bogor,
    df_alert_cimahi,
    df_alert_cirebon,
    df_alert_depok,
    df_alert_sukabumi,
    df_alert_tasikmalaya,
) = lst_pivot_df

# concat all dfs
df_alert_merge = pd.concat(
    [
        df_alert_bandung,
        df_alert_banjar,
        df_alert_bekasi,
        df_alert_bogor,
        df_alert_cimahi,
        df_alert_cirebon,
        df_alert_depok,
        df_alert_sukabumi,
        df_alert_tasikmalaya,
    ],
    ignore_index=True,
)
df_alert_merge.sort_values(["Date"], inplace=True)
df_alert_merge.reset_index(drop=True, inplace=True)

# convert to csv file
df_alert_merge.to_csv(
    os.path.join(os.getcwd(), "data/processed_data/time_series_alert_records.csv"),
    index=False,
)
