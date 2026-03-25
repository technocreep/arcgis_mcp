from fastapi import APIRouter, Request, Depends
from pydantic import BaseModel
from pathlib import Path
from _backend.data_utils.data_reader_utils import read_blocks, merge_block_and_some_data_to_viz_data
from _backend.projects_utils.load_projects import PROJECTS_DIR


router = APIRouter(
    prefix="/dataAPI",
    tags=["dataAPI"],
    #dependencies=[Depends(verify_user)]
)


class RequestDataPayload(BaseModel):
    project_id: str


@router.post("/get_current_data")
async def get_current_data(request: RequestDataPayload):
    """Return viz data for project's datacube by `project_id`.

    Expected files inside `<PROJECTS_DIR>/<project_id>/datacube/`:
      - blocks.csv
      - scores.csv
      - labels.csv
      - features.csv
    """
    project_id = request.project_id
    base = Path(PROJECTS_DIR) / project_id / "datacube"

    block_file = base / "blocks.csv"
    scores_file = base / "scores.csv"
    labels_file = base / "labels.csv"
    features_file = base / "features.csv"

    missing = [str(p) for p in (block_file, scores_file, labels_file, features_file) if not p.exists()]
    if missing:
        return {"status": 1, "message": f"Required files not found: {missing}"}

    try:
        block_data = read_blocks(str(block_file))
        scores_data = read_blocks(str(scores_file))
        labels_data = read_blocks(str(labels_file))
        features_data = read_blocks(str(features_file))
    except FileNotFoundError as fe:
        return {"status": 1, "message": str(fe)}
    except Exception as e:
        return {"status": 2, "message": f"Failed to read data: {e}"}

    features_fields = [
        "grav_field",
        "mag_field",
        "grav_grad",
        "mag_grad",
        "extr_pos",
        "extr_neg",
        "geo_unit",
        "minerag_unit",
        "dist_fault_any_m",
        "dist_contact_m",
        "fault_cnt_r10000",
        "fault_len_r10000",
        "contact_rough_r10000",
    ]

    try:
        data_example = merge_block_and_some_data_to_viz_data(
            block_data,
            [scores_data, labels_data, features_data],
            [["score"], ["label_y", "dist_nearest_ore_m", "weight_w"], features_fields],
        )
    except Exception as e:
        return {"status": 3, "message": f"Failed to merge data: {e}"}

    return {"status": 0, "data": data_example}


