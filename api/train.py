from fastapi import APIRouter, HTTPException

from command.pgy.打标签 import automatic_product
from command.培训用.定时.SendYaoyue import automatic_product_01, update_yaoyue_total, get_table_content

router = APIRouter()

@router.get("/run/automatic_product")
async def run_automatic_product():
    try:
        results = automatic_product()
        return {"status": "success", "messages": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/run/automatic_product_01")
async def run_automatic_product_01():
    try:
        results = automatic_product_01()
        return {"status": "success", "messages": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/run/update_yaoyue")
async def run_update_yaoyue():
    try:
        results = update_yaoyue_total()
        return {"status": "success", "messages": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/run/send_yaoyue")
async def run_send_yaoyue():
    try:
        results = get_table_content()
        return {"status": "success", "messages": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))