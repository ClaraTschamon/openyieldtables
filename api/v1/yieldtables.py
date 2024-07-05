from fastapi import APIRouter, HTTPException, Request

from api.models.exceptions import HTTPNotFoundError
from api.v1.send_response import send_response
from openyieldtables.models.yieldtable import YieldClass, YieldTable
from openyieldtables.yieldtables import get_yield_table, get_interpolated_yield_table

router = APIRouter(
    prefix="/v1/yield-tables",
    tags=["YieldTables"],
)


@router.get(
    "/{yield_table_id}",
    response_model=YieldTable,
    responses={
        404: {
            "description": "Yield table not found",
            "model": HTTPNotFoundError,
        }
    },
)
def read_yield_table_data(yield_table_id: int, request: Request):
    try:
        return send_response(
            get_yield_table(yield_table_id), "yield-table.html", request
        )
    except ValueError:
        raise HTTPException(
            status_code=404,
            detail={
                "message": f"Yield table with ID {yield_table_id} not found."
            },
        )


@router.get(
    "/{yield_table_id}/interpolated/{interpolationvalue}",
    response_model=YieldClass,
    responses={
        404: {
            "description": "Interpolated yield table not found",
            "model": HTTPNotFoundError,
        }
    },
)
def read_interpolated_yield_table_data(yield_table_id: int, interpolation_value: float, request: Request):
    try:
        return send_response(
            get_interpolated_yield_table(yield_table_id, interpolation_value), "yield-table.html", request
        )
    except ValueError:
        raise HTTPException(
            status_code=404,
            detail={
                "message": f"Yield class {interpolation_value} not found."
            },
        )
