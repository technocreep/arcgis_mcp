from fastapi import APIRouter, Request, Depends
from pydantic import BaseModel
import secrets
from _backend.data_utils.data_reader_utils import read_blocks, merge_block_and_some_data_to_viz_data
from os.path import join, exists


router = APIRouter(
    prefix="/modelAPI",
    tags=["modelAPI"],
    #dependencies=[Depends(verify_user)]
)

class RequestDataPayload(BaseModel):
    folder_name: str
