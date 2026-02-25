from uuid import uuid4

from fastapi import APIRouter
from schemas.ingest import IngestRequest
from workers.celery_tasks import process_document_task
from core.redis_client import redis_manager


router = APIRouter()


@router.post("/ingest")
async def ingest(payload: IngestRequest):
    job_id = str(uuid4())

    redis_manager.set_job_status(job_id, "processing")

    process_document_task.delay(
        job_id,
        payload.model_dump()
    )

    return {
        "status": "accepted",
        "job_id": job_id,
        "message": f"Job {job_id} has been queued for background processing and webhook delivery"
    }