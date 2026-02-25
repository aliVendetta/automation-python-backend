import logging
import requests
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

WEBHOOK_URL = "https://hook.eu1.make.com/gxhv22brpghf60o7rjff8l8kxuoagybx"

def send_consolidated_webhook(job_id: str, payload_type: str, data: Any, delivery_id: Optional[str] = None) -> bool:
    """
    Sends a webhook to Make.com with retry logic.
    Can be used for both sequential (one row) and batch (all results) delivery.
    """
    payload = {
        "job_id": job_id,
        "delivery_id": delivery_id or f"direct_{int(time.time())}",
        "payload_type": payload_type, # "single_row" or "job_summary"
        **data
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Sending {payload_type} webhook for JobID: {job_id} (Attempt {attempt+1}/{max_retries}) | DeliveryID: {payload['delivery_id']}")
            response = requests.post(WEBHOOK_URL, json=payload, timeout=30)
            
            if response.status_code in [200, 201, 202]:
                logger.info(f"Webhook acknowledged by Make.com for JobID: {job_id}. Status: {response.status_code}")
                return True
            else:
                logger.warning(f"Make.com Webhook FAILED for JobID {job_id}. Status: {response.status_code}, Body: {response.text}")
        except Exception as e:
            logger.error(f"Error sending webhook for JobID {job_id}: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(5 * (attempt + 1)) # Simple wait between retries
            
    return False
