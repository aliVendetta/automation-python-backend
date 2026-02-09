from fastapi import APIRouter, HTTPException
from workers.state import JOB_RESULTS

router = APIRouter()

@router.get("/result/{job_id}")
async def get_result(job_id: str):
    if job_id not in JOB_RESULTS:
        raise HTTPException(status_code=404, detail="Not ready")

    return {
        "data": [JOB_RESULTS[job_id]]
    }
