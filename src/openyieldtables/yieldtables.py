import csv
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, TypedDict, Union, cast

from openyieldtables.models.yieldtable import TreeType

from .models import (
    YieldClass,
    YieldClassRow,
    YieldTable,
    YieldTableData,
    YieldTableMeta,
)
from .utils import find_available_columns, parse_float

# Construct the path to the CSV files dynamically
script_location = Path(__file__).resolve().parent
csv_path_meta = script_location / "data" / "yield_tables_meta.csv"
csv_path_yield_tables = script_location / "data" / "yield_tables.csv"


class YieldTableMetaCSVRow(TypedDict, total=False):
    id: int
    title: str
    country_codes: List[str]
    type: Optional[str]
    source: str
    link: Optional[str]
    yield_class_step: Optional[float]
    age_step: Optional[int]
    available_columns: List[str]
    tree_type: TreeType


def get_yield_tables_meta() -> List[YieldTableMeta]:
    """
    Reads the yield tables metadata from `data/yield_tables_meta.csv` and
    returns a list of YieldTableMeta instances.

    The CSV file is expected to be in a specific format, with columns for `id`,
    `title`, `country_codes`, `type`, `source`, `link`, etc.

    Returns:
        List[YieldTableMeta]: A list of `YieldTableMeta` instances, one for
            each row in the CSV file.
    """

    yield_table_meta_list = []

    with open(
        csv_path_meta,
        mode="r",
        encoding="utf-8",
    ) as csv_file:
        reader = csv.DictReader(csv_file, delimiter=";")

        for row in reader:
            csv_row = cast(
                YieldTableMetaCSVRow,
                {
                    "id": int(row.get("id", 0)),
                    "title": row.get("title", ""),
                    "country_codes": (
                        row.get("country_codes", "").split(",")
                        if row.get("country_codes")
                        else []
                    ),
                    "type": row.get("type"),
                    "source": row.get("source", ""),
                    "link": row.get("link", ""),
                    "yield_class_step": (
                        float(row["yield_class_step"])
                        if row.get("yield_class_step")
                        else None
                    ),
                    "age_step": (
                        int(row["age_step"]) if row.get("age_step") else None
                    ),
                    "tree_type": row.get("tree_type"),
                    "available_columns": find_available_columns(
                        csv_path_yield_tables,
                        "id",
                        int(row["id"]),
                    ),
                },
            )

            yield_table_meta = YieldTableMeta(**csv_row)
            yield_table_meta_list.append(yield_table_meta)

    return yield_table_meta_list


def get_yield_table_meta(id: int) -> YieldTableMeta:
    """
    Reads the yield table metadata for a specific yield table ID from
     `data/yield_tables_meta.csv` and returns a YieldTableMeta instance.

    The CSV file is expected to be in a specific format, with columns for `id`,
    `title`, `country_codes`, `type`, `source`, `link`, etc.

    Args:
        id (int): The ID of the yield table to get the metadata for.

    Raises:
        ValueError: If the yield table with the specified ID is not found.

    Returns:
        YieldTableMeta: A `YieldTableMeta` instance for the specified yield
            table ID.
    """

    yield_table_meta_list = get_yield_tables_meta()

    for yield_table_meta in yield_table_meta_list:
        if yield_table_meta.id == id:
            return yield_table_meta

    raise ValueError(f"Yield table with ID {id} not found.")


def get_yield_table(id: int) -> YieldTable:
    """
    Reads the yield table data for a specific yield table ID from
    `data/yield_tables.csv` and returns a YieldTable instance.

    The CSV file is expected to be in a specific format, with columns for
    `id`, `yield_class`, `age`, `dominant_height`, `average_height`, etc.

    Args:
        id (int): The ID of the yield table to get the data for.

    Returns:
        YieldTable: A `YieldTable` instance for the specified yield table ID.
    """

    # Get the meta data
    yield_table_meta = get_yield_table_meta(id)

    with open(
        csv_path_yield_tables,
        mode="r",
        encoding="utf-8",
    ) as csv_file:
        reader = csv.DictReader(csv_file, delimiter=";")
        filtered_data = [row for row in reader if int(row["id"]) == id]

    # Organizing data into YieldClasses
    yield_classes_dict: Dict[Union[int, float], List[YieldClassRow]] = {}
    for row in filtered_data:
        # Handle the case where yield_class is a float
        try:
            yield_class: Union[int, float] = int(row["yield_class"])
        except ValueError:
            yield_class = float(row["yield_class"])
        if yield_class not in yield_classes_dict:
            yield_classes_dict[yield_class] = []
        yield_classes_dict[yield_class].append(
            YieldClassRow(
                age=int(row["age"]),
                dominant_height=parse_float(row.get("dominant_height")),
                average_height=parse_float(row.get("average_height")),
                dbh=parse_float(row.get("dbh")),
                taper=parse_float(row.get("taper")),
                trees_per_ha=parse_float(row.get("trees_per_ha")),
                basal_area=parse_float(row.get("basal_area")),
                volume_per_ha=parse_float(row.get("volume_per_ha")),
                average_annual_age_increment=parse_float(
                    row.get("average_annual_age_increment")
                ),
                total_growth_performance=parse_float(
                    row.get("total_growth_performance")
                ),
                current_annual_increment=parse_float(
                    row.get("current_annual_increment")
                ),
                mean_annual_increment=parse_float(
                    row.get("mean_annual_increment")
                ),
            )
        )

    yield_classes = [
        YieldClass(yield_class=yc, rows=rows)
        for yc, rows in yield_classes_dict.items()
    ]

    return YieldTable(
        **yield_table_meta.model_dump(),
        data=YieldTableData(yield_classes=yield_classes),
    )


def get_interpolated_yield_table(id: int, target_yield_class: float) -> YieldClass:
    """
    Reads the yield table data for a specific yield table ID and yield class from
    `data/yield_tables.csv` and returns an interpolated YieldClass instance.

    The CSV file is expected to be in a specific format, with columns for
    `id`, `yield_class`, `age`, `dominant_height`, `average_height`, etc.

    Args:
        id: The ID of the yield table to get the data for.
        target_yield_class: The target yield class to interpolate to.

    Returns:
        YieldClass: A `YieldClass` instance for the target yield class.

    """

    # Make a list of yield class rows
    lower_yield_class = int(target_yield_class)
    upper_yield_class = lower_yield_class + 1

    yield_table_lower_class = get_yield_table_of_class(id, lower_yield_class)
    yield_table_upper_class = get_yield_table_of_class(id, upper_yield_class)

    # List of yield class rows
    interpolation_table = []

    for lower_row, upper_row in zip(yield_table_lower_class, yield_table_upper_class):
        interpolation_table.append(
            YieldClassRow(
                age=int(lower_row.age),
                dominant_height=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.dominant_height, upper_row.dominant_height]
                ),
                average_height=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.average_height, upper_row.average_height]
                ),
                dbh=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.dbh, upper_row.dbh]
                ),
                taper=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.taper, upper_row.taper]
                ),
                trees_per_ha=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.trees_per_ha, upper_row.trees_per_ha]
                ),
                basal_area=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.basal_area, upper_row.basal_area]
                ),
                volume_per_ha=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.volume_per_ha, upper_row.volume_per_ha]
                ),
                average_annual_age_increment=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.average_annual_age_increment, upper_row.average_annual_age_increment]
                ),
                total_growth_performance=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.total_growth_performance, upper_row.total_growth_performance]
                ),
                current_annual_increment=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [safe_float(lower_row.current_annual_increment), safe_float(upper_row.current_annual_increment)]
                ),
                mean_annual_increment=np.interp(
                    target_yield_class, [lower_yield_class, upper_yield_class], [lower_row.mean_annual_increment, upper_row.mean_annual_increment]
                ),
            )
        )

    return YieldClass(yield_class=target_yield_class, rows=interpolation_table)


def get_yield_table_of_class(id: int, yield_class: int) -> YieldClassRow:
    # Get the yield table
    yield_table = get_yield_table(id)

    # Find the yield class
    for yield_class_data in yield_table.data.yield_classes:
        if yield_class_data.yield_class == yield_class:
            return yield_class_data.rows

    raise ValueError(f"Yield class {yield_class} not found in yield table {id}.")


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

