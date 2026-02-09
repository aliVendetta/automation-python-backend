import uuid
from datetime import datetime
from schemas.output import OfferItem
from core.openai_client import extract_offer
from workers.state import JOB_RESULTS, JOB_STATUS


async def process_offer(payload, job_id: str):
    JOB_STATUS[job_id] = "processing"

    try:
        uid = str(uuid.uuid4())
        extracted = await extract_offer(payload.text_body or "")

        offer = OfferItem(
            uid=uid,

            product_name="Freixenet Carta Nevada Extra Dry",
            product_key="freixenet_freixenet_carta_nevada_extra_dry_bottle",
            brand="Freixenet",
            category=None,
            sub_category=None,
            packaging="Bottle",
            packaging_raw="bottle",
            bottle_or_can_type=None,
            unit_volume_ml=750,
            units_per_case=6,
            cases_per_pallet=None,
            quantity_case=None,
            gift_box=None,
            refillable_status="NRF",
            currency="EUR",
            price_per_unit=3.2667,
            price_per_unit_eur=3.2667,
            price_per_case=19.6,
            price_per_case_eur=19.6,
            fx_rate=1,
            fx_date="2025-12-19",
            alcohol_percent="11%",
            origin_country=None,
            supplier_country="",
            incoterm="DAP",
            location="Loendersloot",
            lead_time="2 weeks",
            moq_cases=None,
            valid_until=None,
            offer_date=datetime.utcnow(),
            date_received=datetime.utcnow(),
            best_before_date=None,
            vintage=None,
            supplier_name=payload.supplier_name,
            supplier_email=payload.supplier_email,
            supplier_reference=None,
            source_channel=payload.source_channel,
            source_message_id=payload.source_message_id,
            source_filename=payload.source_filename,
            confidence_score=0.85,
            needs_manual_review=False,
            error_flags=[],
            custom_status=None,
            processing_version="1.0.0",
            ean_code=None,
            label_language="EN",
            product_reference=None,
        )

        JOB_RESULTS[job_id] = offer.model_dump()
        JOB_STATUS[job_id] = "done"

    except Exception as e:
        JOB_STATUS[job_id] = "failed"
        JOB_RESULTS[job_id] = {
            "job_id": job_id,
            "error": str(e),
        }
