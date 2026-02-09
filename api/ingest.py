from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks
from schemas.ingest import IngestRequest
from schemas.output import OfferResponse
from workers.processor import process_offer

router = APIRouter()

@router.post("/ingest")
async def ingest(payload: IngestRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid4())

    background_tasks.add_task(
        process_offer,
        payload,
        job_id
    )

    return {
        "status": "accepted",
        "job_id": job_id
    }