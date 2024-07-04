import json
import os
import time
from pathlib import Path

import geopandas as gpd
import pandas as pd
import urllib3
from dotenv import load_dotenv

from create_data_logger import logger

load_dotenv()


def create_asset_gdf(assets_csv: str, continents_gpkg: str):
    """
    Creates a GeoDataFrame by joining asset data with continent data.

    Parameters:
    - assets_csv (str): The file path to the CSV file containing asset data.
    - continents_gpkg (str): The file path to the GeoPackage file with continent data.

    Returns:
    - GeoDataFrame: A GeoDataFrame containing the asset data with an additional
    'CONTINENT' column, where each asset is assigned to a continent or labeled as
    'Global' if no match is found.
    """
    logger.info("Creating asset geodataframe.")
    # read in the global power plant database
    assets_df = pd.read_csv(assets_csv, low_memory=False)
    # convert the lat/long to a geodataframe
    assets_gdf = gpd.GeoDataFrame(
        assets_df, geometry=gpd.points_from_xy(assets_df.longitude, assets_df.latitude)
    )
    # Set crs on the geodataframe
    assets_gdf.crs = "epsg:4326"
    # read in the continents
    continents_gdf = gpd.read_file(continents_gpkg)
    continents_gdf.to_crs("epsg:4326", inplace=True)
    # join the power plant data to the continents
    assets_with_continent = gpd.sjoin(
        assets_gdf, continents_gdf[["CONTINENT", "geometry"]], how="left"
    )
    assets_with_continent["CONTINENT"] = assets_with_continent["CONTINENT"].fillna(
        "Global"
    )
    if "index_right" in assets_with_continent.columns:
        assets_with_continent.drop(columns=["index_right"], inplace=True)
    return assets_with_continent


def create_sample_gdf(assets_gdf: gpd.GeoDataFrame, num_samples: int, asset_class: str):
    """
    Creates a sample DataFrame from a GeoDataFrame of assets based on the specified
    asset class.

    This function selects a random sample of assets from the provided GeoDataFrame
    according to the specified number of samples. It then assigns the specified asset
    class to all selected samples and determines the asset type based on theclass.

    Parameters:
    - assets_gdf (gpd.GeoDataFrame): The GeoDataFrame containing asset data.
    - num_samples (int): The number of samples to select from the GeoDataFrame.
    - asset_class (str): The class of assets to assign to the samples.

    Returns:
    - sample_df (pd.DataFrame): A DataFrame containing the selected samples with added
    'asset_class' and 'asset_type' columns based on the specified asset class.

    The function supports the following asset classes and assigns asset types as follows:
    - "PowerGeneratingAsset": 'asset_type' is set to the value in 'primary_fuel'.
    - "ThermalPowerGeneratingAsset": 'asset_type' is set to "Gas".
    - "RealEstateAsset": 'asset_type' is set to "Buildings/Industrial".
    - "IndustrialActivity": 'asset_type' is set to "Construction".
    """
    logger.info(
        "Creating sample dataframe for %s for %s assets.", asset_class, num_samples
    )
    sample_df = assets_gdf.sample(n=num_samples)
    sample_df["asset_class"] = asset_class
    match asset_class:
        case "PowerGeneratingAsset":
            sample_df["asset_type"] = sample_df["primary_fuel"]
        case "ThermalPowerGeneratingAsset":
            sample_df["asset_type"] = "Gas"
        case "RealEstateAsset":
            sample_df["asset_type"] = "Buildings/Industrial"
        case "IndustrialActivity":
            sample_df["asset_type"] = "Construction"
    return sample_df


def create_request_json(
    assets_gdf: gpd.GeoDataFrame, asset_class: str, no_of_rows: int, output_file: str
):
    sample_gdf = create_sample_gdf(assets_gdf, no_of_rows, asset_class)
    geojson = {
        "type": "FeatureCollection",
        "features": [],
        "properties": {
            "include_asset_level": True,
            "include_calc_details": True,
            "include_measures": True,
            "years": [2030, 2040, 2050],
            "scenarios": ["ssp126", "ssp245", "ssp585"],
        },
    }
    gdf = sample_gdf.sample(no_of_rows)
    for index, row in gdf.iterrows():
        feature = {
            "type": "Feature",
            "properties": {
                "asset_class": row["asset_class"],
                "type": row["asset_type"],
                "location": row["CONTINENT"],
            },
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]],
            },
        }
        geojson["features"].append(feature)
    geojson_string = json.dumps(geojson)  # .replace('"', '\\"')
    request_dict = {"inputs": {"workspace": "ddowding", "json_string": geojson_string}}
    with open(output_file, "w") as f:
        json.dump(request_dict, f)
    return request_dict


def get_result(request_dict: dict, result_file: str, job_id_file: str):
    logger.info("Sending request to asset impact workflow.")
    http = urllib3.PoolManager(cert_reqs="CERT_NONE")
    urllib3.disable_warnings()
    auth_dict = urllib3.make_headers(basic_auth=os.getenv("LOGIN_DETAILS"))
    process_name = "get-asset-impact-workflow-batch"
    user = "eric"
    ades_endpoint = "test.eodatahub.org.uk/ades"
    execution_url = (
        f"https://{ades_endpoint}/{user}/ogc-api/processes/{process_name}/execution"
    )
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Prefer": "respond-async",
    }
    headers.update(auth_dict)

    response = http.request(
        "POST", execution_url, headers=headers, body=json.dumps(request_dict)
    )
    executeStatus = response.headers["Location"]
    json.loads(response.data)
    response_json = json.loads(response.data)
    jobID = response_json["jobID"]
    with open(job_id_file, "w") as f:
        f.write(jobID)
    logger.info("Job ID: %s", jobID)
    headers = {"Accept": "application/json"}
    headers.update(auth_dict)
    status = "not run"
    loop_counter = 0
    logger.info("Waiting for job to complete.")
    while status != "successful":
        loop_counter += 1
        logger.info("Loop counter: %s", loop_counter)
        response = http.request("GET", executeStatus, headers=headers)
        status = json.loads(response.data)["status"]
        time.sleep(5)
    results_url = f"https://ddowding.workspaces.test.eodhp.eco-ke-staging.com/files/eodhp-test-workspaces1/processing-results/cat_{jobID}.json"
    token = os.getenv("RESULTS_TOKEN")
    headers = {"Accept": "application/json"}
    # add bearer token to headers
    headers.update({"Authorization": f"Bearer {token}"})
    logger.info("Getting results")
    result_response = http.request("GET", results_url, headers=headers)
    result_json = result_response.json()
    with open(result_file, "w", encoding="utf-8") as f:
        json.dump(result_json, f)
    return jobID
