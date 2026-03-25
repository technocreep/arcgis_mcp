from pathlib import Path
from typing import Optional, Dict, Any
import csv
import pandas as pd
from os.path import join 
import json
from typing import List
from shapely.geometry import Polygon
from shapely.affinity import translate, scale
import math

def read_blocks(path: Optional[str]) -> Dict[str, Dict[str, Any]]:

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"blocks.csv not found at: {p}")

    result: Dict[str, Dict[str, Any]] = {}

    df = pd.read_csv(path)
    columns = df.columns.tolist()

    id_column = "block_id"
    rest_columns = [col for col in columns if col != id_column]

    for _, row in df.iterrows():
        block_id = row[id_column]
        block_data = {col: row[col] for col in rest_columns}
        result[block_id] = block_data

    return result


def merge_block_and_some_data_to_viz_data(blocks: Dict[str, Dict[str, Any]], some_data_list: List[Dict[str, Dict[str, Any]]], fields: List[str]):
    base_rect_point_1 = [blocks["block_0_0"]["lon"], blocks["block_0_0"]["lat"]]
    base_rect_point_2 = [blocks["block_0_1"]["lon"], blocks["block_0_1"]["lat"]]
    base_rect_point_3 = [blocks["block_1_1"]["lon"], blocks["block_1_1"]["lat"]]
    base_rect_point_4 = [blocks["block_1_0"]["lon"], blocks["block_1_0"]["lat"]]
    base_polygon = Polygon([base_rect_point_1, base_rect_point_2, base_rect_point_3, base_rect_point_4])
    c = base_polygon.centroid
    base_polygon = translate(base_polygon, xoff=-c.x, yoff=-c.y)
    base_polygon = scale(base_polygon, xfact=1.02, yfact=1.01, origin=(0, 0))  


    data = []
    data_keys = list(blocks.keys())
    for key in data_keys:
        lat = blocks[key].get("lat")
        lon = blocks[key].get("lon")
        temp_polygon = translate(base_polygon, xoff=lon, yoff=lat)
        temp_item = {
            "id": key,
            "polygon": temp_polygon.wkt,  
            "lat": lat,
            "lon": lon
        }
        for i in range(len(some_data_list)):
            some_data = some_data_list[i]
            for item in fields[i]:
                if key not in some_data:
                    raise ValueError(f"Key {key} from blocks not found in some_data")
                value = some_data[key].get(item)
                if isinstance(value, float) and math.isnan(value):
                    value = 0
                temp_item[item] = value
        data.append(temp_item)
    return data

if __name__ == "__main__":
    # quick local check: read blocks and print summary
    path = "/run/media/karl/BIG_SSD/Data_cube/output_lekyn"

    block_file = join(path, "blocks.csv")

    block_data = read_blocks(block_file)

    scores_file = join(path, "scores.csv")
    scores_data = read_blocks(scores_file)

    data_example = merge_block_and_some_data_to_viz_data(block_data, [scores_data], [["score"]])

    with open(join(path, "viz_data.json"), "w") as f:
        json.dump(data_example, f, indent=2)


