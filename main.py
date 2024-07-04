from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from create_data_logger import logger
from methods import create_asset_gdf, create_request_json, get_result

ASSETS_CSV = "/Users/sgdowd/Documents/1_EODH/data/global_power_plant_database_with_assetcloss.csv"
CONTINENTS_GPKG = "./World_Continents_6670117379411214938.gpkg"
ASSET_CLASSES = [
    "PowerGeneratingAsset",
    "ThermalPowerGeneratingAsset",
    "RealEstateAsset",
    "IndustrialActivity",
]
NOS_OF_ROWS = [10, 500]
EXPERIMENT_NO = 1
BASE_DIR = Path("./data")


def process_asset(asset_class, no_of_rows, assets_gdf, experiment_no, base_dir):
    logger.info(
        "Running experiment %s for %s with %s rows",
        experiment_no,
        asset_class,
        no_of_rows,
    )
    asset_dir = base_dir / asset_class
    asset_dir.mkdir(exist_ok=True)
    output_dir = asset_dir / f"e{experiment_no}_{asset_class}_{no_of_rows}"
    output_dir.mkdir(exist_ok=True)
    request_dict = create_request_json(
        assets_gdf=assets_gdf,
        asset_class=asset_class,
        no_of_rows=no_of_rows,
        output_file=output_dir / "input.json",
    )
    job_id = get_result(
        request_dict,
        output_dir / f"output_{asset_class}_{no_of_rows}.json",
        output_dir / "job_id.txt",
    )
    return job_id


if __name__ == "__main__":
    logger.info("Starting the process.")
    assets_gdf = create_asset_gdf(ASSETS_CSV, CONTINENTS_GPKG)
    # Prepare arguments for each task
    tasks = [
        (asset_class, no_of_rows, assets_gdf, EXPERIMENT_NO, BASE_DIR)
        for asset_class in ASSET_CLASSES
        for no_of_rows in NOS_OF_ROWS
    ]
    # Execute tasks concurrently
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_asset, *task) for task in tasks]

        # Optionally, process results as they complete
        for future in as_completed(futures):
            result = future.result()
            logger.info("Task completed with result id of %s", result)
