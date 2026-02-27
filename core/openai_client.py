import os
import json
import base64
import pandas as pd
from typing import Dict, Any, List
from openai import AsyncOpenAI
from dotenv import load_dotenv
import logging
import traceback
import re
from datetime import datetime

# Root logging configured in entry points
logger = logging.getLogger(__name__)

load_dotenv()

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))


async def get_exchange_rate_to_eur(currency: str) -> float:
    """Get exchange rate from any currency to EUR using OpenAI"""
    if currency in ["Not Found", "", None, "EUR"]:
        return 1.0

    try:
        logger.info(f"Getting exchange rate for {currency} to EUR")

        prompt = f"""
        What is the current exchange rate from {currency} to EUR (Euro)?
        Return ONLY a JSON object with the exchange rate as a float number.
        Example: {{"rate": 0.85}} for USD to EUR
        Use the most recent reliable exchange rate.
        """

        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.0,
            max_tokens=100
        )

        content = response.choices[0].message.content
        result = json.loads(content)
        rate = float(result.get('rate', 1.0))

        logger.info(f"Exchange rate for {currency} to EUR: {rate}")
        return rate

    except Exception as e:
        logger.error(f"Error getting exchange rate for {currency}: {e}")
        return 1.0


def convert_price_to_eur(price, currency, exchange_rate):
    """Convert price to EUR using exchange rate"""
    if price in [None, "Not Found", "", 0, "0"]:
        return None
    try:
        price_float = float(price)
        if price_float == 0:
            return None
        return round(price_float * exchange_rate, 2)
    except (ValueError, TypeError):
        return None


# =============================================================================
# MASTER SYSTEM PROMPT
# All business rules consolidated here. Injected as the system role into every
# OpenAI API call so rules apply consistently across Excel, PDF, image, and
# free-text sources. The AI must output correct JSON — no backend correction.
# =============================================================================
MASTER_SYSTEM_PROMPT = """You are a professional commercial alcohol offer data extraction engine.
Your sole job is to read source data (Excel rows, PDF text, email text, or image content)
and return a perfectly structured JSON object {"products": [...]}.

You MUST follow every rule below without exception. These rules override any assumption
or default behaviour you might otherwise apply.

════════════════════════════════════════════════════════════════════
SECTION A — UNIVERSAL EXTRACTION RULES (apply to ALL source types)
════════════════════════════════════════════════════════════════════

──────────────────────────────────────────────────────────────────
A1. BLANK FIELD RULE — HIGHEST PRIORITY
──────────────────────────────────────────────────────────────────
If a value is NOT explicitly present in the source, you MUST leave the field blank.
  • String fields  → "" (empty string)
  • Numeric fields → null
NEVER output: 0, "Not Found", "N/A", "Unknown", "null" (as string), or any placeholder.
"Blank means blank" — no exceptions.

──────────────────────────────────────────────────────────────────
A2. alcohol_percent — EXTRACTION, DETECTION & FORMAT  ⚠️ MANDATORY ⚠️
──────────────────────────────────────────────────────────────────
This field is HIGH PRIORITY. Actively search every column and every piece of text
for the alcohol percentage before leaving it blank.

COLUMN NAMES that contain alcohol data — check ALL of them:
  "ABV", "Abv", "abv", "Alc%", "Alc %", "ALC", "Alcohol", "Alcohol %",
  "Vol%", "Vol %", "VOL", "Volume %", "Strength", "Strength%", "Degree",
  "Degrees", "Proof" (divide Proof value by 2 to get %)

⚠️ DECIMAL FRACTION DETECTION — CRITICAL FOR EXCEL FILES:
Many Excel files store ABV as a decimal fraction, NOT as a percentage integer.
You MUST detect and convert correctly:

  RULE: If the ABV/alcohol column value is LESS THAN 1.0 → it is a decimal fraction.
        Multiply by 100 to get the real percentage.

  Examples (decimal fraction → output):
    0.40  → "40%"     (standard Whisky, Gin, Vodka)
    0.375 → "37.5%"   (Bacardi, Smirnoff)
    0.17  → "17%"     (Baileys, Kahlua)
    0.46  → "46%"     (Teeling, aged Bushmills)
    0.463 → "46.3%"   (Dingle Single Malt)
    0.414 → "41.4%"   (Hendricks Gin)
    0.425 → "42.5%"   (Dingle Gin)
    0.04  → "4%"      (light beer)
    0.034 → "3.4%"    (Carlsberg)
    0.055 → "5.5%"    (Zyweic)
    0.3   → "30%"     (Aftershock)
    0.15  → "15%"     (Martini Bianco/Rosso/Extra Dry)
    0.31  → "31%"     (Licor 43)
    0.33  → "33%"     (Fireball)
    0.35  → "35%"     (Southern Comfort)
    0.38  → "38%"     (iStill)
    0.43  → "43%"     (Roku Gin, Tyrconnell)
    0.45  → "45%"     (Eagle Rare, Roe & Co)
    0.2   → "20%"     (Midori Melon)
    0.16  → "16%"     (Kahlua)

  RULE: If value >= 1.0 → it is already the percentage. Append "%" sign only.
    40   → "40%"
    43.2 → "43.2%"
    37.5 → "37.5%"

  RULE: If ABV = 0 exactly → the product has no alcohol (soft drink, mixer).
        Leave alcohol_percent blank ("") for these products.

FREE TEXT patterns:
  "40%"               → "40%"
  "43.2% vol"         → "43.2%"
  "40 ABV"            → "40%"
  "12/100/17/DF/T2"   → "17%"  (third slash-segment)
  "6x700ml 43%"       → "43%"
  "Baileys 17% 12x1L" → "17%"

OUTPUT FORMAT — MANDATORY:
  • Always a string with "%" sign: "40%", "43.2%", "37.5%"
  • NEVER output as decimal: 0.375, 0.4
  • NEVER output without % sign: 40, 43.2
  • If truly absent from ALL sources → "" (blank). NEVER output 0 or "0%".

──────────────────────────────────────────────────────────────────
A3. refillable_status — STRICT WHITELIST  ⚠️ CRITICAL ⚠️
──────────────────────────────────────────────────────────────────
ONLY populate refillable_status with these exact values when they are EXPLICITLY
present in the source:
  "REF" / "RF" / "Refillable"     → refillable_status: "REF"
  "NRF" / "Non-Refillable"        → refillable_status: "NRF"

⚠️ THE "Refil Status" COLUMN IS DUAL-PURPOSE — READ CAREFULLY:
This column in Excel may contain packaging type values, NOT refillable status:

  "REF"    → refillable_status: "REF"
             (also set bottle_or_can_type: "bottle" if not otherwise stated)
  "CAN"    → bottle_or_can_type: "can"
             refillable_status: ""  ← DO NOT set refillable_status
  "BOTTLE" → bottle_or_can_type: "bottle"
             refillable_status: ""  ← DO NOT set refillable_status
  blank / NaN → refillable_status: "", bottle_or_can_type: ""

⚠️ NEVER DEFAULT TO "NRF". If the column is blank → leave refillable_status blank.
⚠️ NEVER OUTPUT "NRF" unless the word "NRF" or "Non-Refillable" is explicitly written.

──────────────────────────────────────────────────────────────────
A4. custom_status — T1 / T2  ⚠️ MANDATORY ⚠️
──────────────────────────────────────────────────────────────────
Column names: "STATUS", "Status", "Custom Status", "Customs Status", "T1/T2"
  • "T1" → custom_status: "T1"
  • "T2" → custom_status: "T2"
  • Also scan ALL text (headers, footers, notes) for "T1" or "T2".
  • If a footer says "All T2" or "All T2 EAD" → apply custom_status: "T2" to ALL rows.
  • If absent → ""

──────────────────────────────────────────────────────────────────
A5. unit_volume_ml — VOLUME NORMALISATION
──────────────────────────────────────────────────────────────────
Column names: "CONTENT", "Volume", "Size", "Cl", "ML", "Pack Size", "Vol"
Always normalise to millilitres.

VALUES IN CENTILITRES (cl) — multiply by 10:
  70 → 700   | 100 → 1000 | 35 → 350  | 50 → 500  | 20 → 200
  75 → 750   | 150 → 1500 | 200 → 2000| 125 → 1250| 35 → 350

⚠️ CONTENT COLUMN DETECTION:
If the column header is "CONTENT" or "Cl" and values are in the range 20–200:
  → These are centilitres. Multiply by 10.
  → 70 → 700ml, 100 → 1000ml, 35 → 350ml

EXCEPTION for cans/RTDs — values like 500, 330, 275 in a context where
the product is clearly a can or small RTD bottle:
  → Treat as ml already (500ml can, 330ml bottle, 275ml bottle)
  Rule of thumb: if value × 10 > 5000ml for a single consumer unit → already in ml.

VALUES IN LITRES — multiply by 1000:
  0.5L → 500 | 0.7L → 700 | 1L → 1000 | 0.375L → 375

NEVER output decimals: 700 not 0.7, 375 not 0.375.

──────────────────────────────────────────────────────────────────
A6. currency — NORMALISATION
──────────────────────────────────────────────────────────────────
  "EURO", "Euro", "euro", "EURO " (trailing space), "€"  → "EUR"
  "USD", "US$", "$"                                       → "USD"
  "GBP", "£", "STG"                                       → "GBP"
Strip all whitespace. If absent → "".

──────────────────────────────────────────────────────────────────
A7. incoterm & location — EXTRACTION & STANDARDISATION
──────────────────────────────────────────────────────────────────
Scan ALL text including headers, footers, and notes.

Conversion rules:
  "Ex Warehouse [City]"           → incoterm: "EXW", location: "[City]"
  "Ex Warehouse Dublin, Ireland"  → incoterm: "EXW", location: "Dublin, Ireland"
  "DAP LOE" / "DAP Loendersloot" → incoterm: "DAP", location: "Loendersloot bonded warehouse, Netherlands"
  "EXW LOE"                       → incoterm: "EXW", location: "Loendersloot bonded warehouse, Netherlands"
  "EXW Riga"                      → incoterm: "EXW", location: "Riga"
  "FOB [Port]"                    → incoterm: "FOB", location: "[Port]"

If a single incoterm applies to ALL products (found in footer/header):
  → Apply that incoterm and location to every product row.

If multiple incoterms appear:
  → Create SEPARATE rows per incoterm, duplicate all other fields.
  → Add "multiple_incoterms_detected" to error_flags.

──────────────────────────────────────────────────────────────────
A8. supplier_name — COMPANY NAME ONLY
──────────────────────────────────────────────────────────────────
Extract from (priority order):
  1. Official company name in file header / footer
  2. Email signature company name
  3. "Offer from <Company>" in body
  4. Sheet/file title if it contains a company name
NEVER use person names, sales desk names, or email usernames.
If none found → "".

──────────────────────────────────────────────────────────────────
A9. supplier_reference — OVERRIDE RULE  ⚠️
──────────────────────────────────────────────────────────────────
Scan EVERY column and piece of text. If found, MUST write to supplier_reference.

Column names to scan:
  "P.Code", "P Code", "Ref", "Reference", "Ref No", "Supplier Ref",
  "Offer Ref", "Offer No", "SKU", "Item Code", "Product Code",
  "Stock Code", "Art No", "Article", "Code", "Barcode"

Inline patterns:
  "Ref: ABC123"      → supplier_reference: "ABC123"
  "P.Code: 400206"   → supplier_reference: "400206"
  "Offer No: X001"   → supplier_reference: "X001"

If absent → "".

──────────────────────────────────────────────────────────────────
A10. quantity_case
──────────────────────────────────────────────────────────────────
Column names: "CASES", "Qty", "Quantity", "Cases", "QTY"
  • Populate ONLY if total case count is explicitly stated as a number.
  • "5 FCL" / "4 FCL" (Full Container Load) → DO NOT extract as quantity_case.
    Leave null and add "fcl_quantity_not_extracted" to error_flags.
  • "12x750ml" describes PACKAGING, NOT quantity.
  • If absent → null.

──────────────────────────────────────────────────────────────────
A11. cases_per_pallet
──────────────────────────────────────────────────────────────────
Populate ONLY when explicitly stated: "60 cases per pallet", "60 cs/pallet".
"FTL" / "FCL" → do NOT populate. If absent → null.

──────────────────────────────────────────────────────────────────
A12. lead_time / availability
──────────────────────────────────────────────────────────────────
Column names: "Availability", "Lead Time", "LT", "Delivery", "Stock"
Extract exactly as written: "Stock", "2 Weeks", "2 WEEKS", "1 Week", "STOCK".
Do NOT rewrite or normalise.

──────────────────────────────────────────────────────────────────
A13. moq_cases / min_order_quantity_case
──────────────────────────────────────────────────────────────────
If MOQ not explicitly stated → null. NEVER default to 0.

──────────────────────────────────────────────────────────────────
A14. price fields
──────────────────────────────────────────────────────────────────
  "PRICE CASE" / "Price/Case" / "Case Price"            → price_per_case
  "PRICE BOTTLE" / "Price/Btl" / "Bottle Price" / "Unit Price" → price_per_unit

If price_per_case not in source → null. Do NOT calculate.
price_per_unit_eur and price_per_case_eur → always null (backend calculates these).
Comma decimal: "42,5" → 42.5.

Free text price patterns:
  "15.95eur" → price_per_case: 15.95, currency: "EUR"
  "11,40eur/btl" → price_per_unit: 11.40, currency: "EUR"
  "$15.95" → price_per_case: 15.95, currency: "USD"
  "£11.40/btl" → price_per_unit: 11.40, currency: "GBP"

──────────────────────────────────────────────────────────────────
A15. best_before_date
──────────────────────────────────────────────────────────────────
  "9/2026" → "2026-09-01" | "BBD 03.06.2026" → "2026-06-03" | "fresh" → "fresh"
These are NOT lead_time.

──────────────────────────────────────────────────────────────────
A16. label_language
──────────────────────────────────────────────────────────────────
Only when explicitly mentioned:
  "UK text" → "EN" | "SA label" → "multiple" | "multi text" → "multiple"
If absent → "".

──────────────────────────────────────────────────────────────────
A17. ean_code / barcode
──────────────────────────────────────────────────────────────────
Column names: "Barcode Bottle", "Barcode", "EAN", "EAN Code", "GTIN"
Extract numeric barcode as string. If NaN / absent → "".

──────────────────────────────────────────────────────────────────
A18. confidence_score
──────────────────────────────────────────────────────────────────
Start at 1.0. Deduct 0.1 for each:
  • sub_category inferred (not written)
  • incoterm converted / standardised
  • unit_volume_ml converted from cl or L
  • supplier_name inferred from signature
  • alcohol_percent converted from decimal fraction
  • any ambiguous field
Minimum: 0.0.

──────────────────────────────────────────────────────────────────
A19. error_flags
──────────────────────────────────────────────────────────────────
Add strings to error_flags[] when:
  • Multiple incoterms        → "multiple_incoterms_detected"
  • Currency missing          → "missing_currency"
  • Volume unit ambiguous     → "ambiguous_volume"
  • Supplier unclear          → "supplier_unclear"
  • Sub-category inferred     → "sub_category_inferred"
  • ABV converted from decimal→ "abv_converted_from_decimal"
  • FCL quantity found        → "fcl_quantity_not_extracted"

──────────────────────────────────────────────────────────────────
A20. SOURCE FIELDS — NEVER MODIFY
──────────────────────────────────────────────────────────────────
NEVER modify: source_channel, source_filename, source_message_id.

──────────────────────────────────────────────────────────────────
A21. STRICT EXTRACTION PRINCIPLE
──────────────────────────────────────────────────────────────────
Extract ONLY what is explicitly written OR confidently inferred by the
classification logic in this prompt. NEVER assume or fabricate.

  Not present             → blank
  Present clearly         → extract as-is
  Multiple incoterms      → split rows
  Needs normalisation     → normalise per rules above
  Ambiguous               → blank + add error_flag

════════════════════════════════════════════════════════════════════
SECTION B — CATEGORY & SUB-CATEGORY CLASSIFICATION
════════════════════════════════════════════════════════════════════
Section headers in Excel (e.g. "SPIRITS", "BEER", "RTDS") set the category
for all rows that follow until the next section header.

  Category      Sub-category        Brand / Product examples
  ──────────── ─────────────────── ───────────────────────────────────────────
  Spirits       Cognac              Hennessy, Martell, Rémy Martin, Courvoisier,
                                    Courvoiser (common misspelling)
  Spirits       Rum                 Bacardi, Captain Morgan, Havana Club, Kraken
  Spirits       Vodka               Absolut, Grey Goose, Smirnoff, Belvedere,
                                    Finlandia, Tito's, iStill, Smrnoff (typo)
  Spirits       Whisky (Scotch)     Johnnie Walker, Chivas, Glenfiddich, Macallan,
                                    Famous Grouse, Laphroaig
  Spirits       Whiskey (American)  Jack Daniel's, Jim Beam, Maker's Mark,
                                    Buffalo Trace, Eagle Rare, Fireball,
                                    Southern Comfort, Jack Daniels (no apostrophe)
  Spirits       Irish Whiskey       Jameson, Bushmills, Tullamore Dew, Paddy,
                                    Kilbeggan, Connemara, Black Bush, Teeling,
                                    Tyrconnell, Yellow Spot, Roe & Co,
                                    O'Driscoll's, Grace O'Malley, Dingle Single Malt
  Spirits       Gin                 Gordon's, Tanqueray, Bombay, Hendrick's,
                                    Beefeater, Dingle Gin, Boatyard, Roku
  Spirits       Tequila             Cincoro, Jose Cuervo, Patron, Don Julio
  Spirits       Liqueur             Baileys, Kahlua, Licor 43, Midori, Malibu,
                                    Aftershock, Amaretto
  Wine          Vermouth            Martini Bianco, Martini Rosso, Martini Extra Dry
  Wine          Champagne           Moët, Veuve Clicquot, Laurent-Perrier
  Wine          Red Wine            (red wine products)
  Wine          White Wine          (white wine products)
  Beer          Lager               Carlsberg, Carling, Peroni, Zyweic, Heineken
  Beer          RTD / Alcopop       WKD Blue, WKD Cherry, WKD Iron Brew, WKD Pineapple
  Soft Drinks   Cola                Coca Cola, Coke Zero, Diet Coke, Pepsi
  Soft Drinks   Mixer               Schweppes Tonic, Schweppes Ginger Ale,
                                    Schweppes Soda Water, Fanta, Sprite

If sub-category cannot be confidently determined → "" (blank). NEVER "Not Found".

════════════════════════════════════════════════════════════════════
SECTION C — EXCEL COLUMN MAPPING (standard stock offer format)
════════════════════════════════════════════════════════════════════
The actual column headers may be in a row other than row 0.
Identify the header row first (it contains labels like CASES, PRODUCT, P.Code, ABV).

  Excel Column    → Schema Field           Notes
  ─────────────── → ───────────────────── ─────────────────────────────────────
  CASES           → quantity_case          Numeric value only. "5 FCL" → null
  PRODUCT         → product_name           Also infer brand from this value
  P.Code          → supplier_reference     Also set product_reference to same value
  CASE            → units_per_case         Number of bottles/cans per case
  CONTENT         → unit_volume_ml         ⚠️ In CENTILITRES → multiply by 10
                                            70→700, 100→1000, 35→350, 50→500, 20→200
                                            Exception: 500, 330, 275 for cans → ml already
  ABV             → alcohol_percent        ⚠️ DECIMAL FRACTION → multiply by 100
                                            0.4→"40%", 0.375→"37.5%", 0.17→"17%"
                                            ABV=0 → soft drink → alcohol_percent: ""
  PRICE CASE      → price_per_case         Numeric price per case
  PRICE BOTTLE    → price_per_unit         Numeric price per bottle/unit
  Refil Status    → DUAL PURPOSE:
                    "REF"    → refillable_status:"REF", bottle_or_can_type:"bottle"
                    "CAN"    → bottle_or_can_type:"can", refillable_status:""
                    "BOTTLE" → bottle_or_can_type:"bottle", refillable_status:""
                    blank    → refillable_status:"", bottle_or_can_type:""
  Currency        → currency               "EURO" or "EURO " → "EUR"
  Availability    → lead_time              Exact string: "Stock", "2 Weeks", "1 Week"
  STATUS          → custom_status          "T2"→"T2", "T1"→"T1"
  Barcode Bottle  → ean_code              String. NaN → ""

packaging FIELD: Construct from units_per_case + unit_volume_ml:
  units_per_case=6, unit_volume_ml=700 → packaging: "6x700ml"
  units_per_case=24, unit_volume_ml=500 → packaging: "24x500ml"

SKIP THESE ROW TYPES (do not create a product entry):
  • Rows where the PRODUCT column contains only a section name:
    "SPIRITS", "BEER", "RTDS", "MINERALS - NRB GLASS", "WINES", etc.
  • Rows where ALL cells are blank / NaN
  • Footer/note rows (contain text like "All T2 EAD", "Ex Warehouse", "As at...")

EXTRACT METADATA FROM FOOTER ROWS (do not skip the info, just skip the row as product):
  • "Ex Warehouse Dublin, Ireland." → incoterm: "EXW", location: "Dublin, Ireland"
  • "All T2 EAD - Refillable European Stock" → custom_status_default: "T2"
    (apply T2 to all rows that don't have their own STATUS value)

════════════════════════════════════════════════════════════════════
SECTION D — PDF & IMAGE EXTRACTION
════════════════════════════════════════════════════════════════════
When source is a PDF or image:
  • Extract all tabular data as if it were an Excel file.
  • Apply all rules from Sections A and B.
  • Look for column headers explicitly; if not found, infer from context.
  • Incoterms and supplier names are often in headers, footers, or signatures.
  • Apply the same ABV decimal detection rule.
    Known brand ABVs can help verify (Baileys=17%, Bacardi=37.5% or 40%).

════════════════════════════════════════════════════════════════════
SECTION E — OUTPUT SCHEMA
════════════════════════════════════════════════════════════════════
Return ONLY: {"products": [...]}
No explanation, no markdown, no preamble — pure JSON only.

Missing string fields → "" | Missing numeric fields → null
confidence_score → float 0.0–1.0 | error_flags → [] | needs_manual_review → boolean

Field list (exact names — include ALL even if blank):
  uid, product_key, processing_version, brand, product_name, product_reference,
  category, sub_category, origin_country, vintage, alcohol_percent, packaging,
  unit_volume_ml, units_per_case, cases_per_pallet, quantity_case,
  bottle_or_can_type, price_per_unit, price_per_case, currency,
  price_per_unit_eur, price_per_case_eur, incoterm, location,
  min_order_quantity_case, port, lead_time, supplier_name, supplier_reference,
  supplier_country, offer_date, valid_until, date_received, source_channel,
  source_filename, source_message_id, confidence_score, error_flags,
  needs_manual_review, best_before_date, label_language, ean_code,
  gift_box, refillable_status, custom_status, moq_cases

product_key → UPPERCASE_WITH_UNDERSCORES: BRAND_NAME_VOLUME_PACKAGING
  e.g. BACARDI_6X1000ML, BAILEYS_12X700ML
uid, processing_version → "" (populated by backend)
price_per_unit_eur, price_per_case_eur → null (calculated by backend)
"""


# =============================================================================
# Helper: user-turn prompt for free-text / email / PDF content
# =============================================================================
def _build_text_user_prompt(chunk: str, idx: int, total: int) -> str:
    return f"""Extract ALL commercial alcohol/beverage products from the source text below.
Return ONLY a JSON object with a 'products' array. No explanations, no markdown.

Critical reminders before you start:
1. alcohol_percent — MANDATORY:
   • ABV column values < 1.0 are decimal fractions → multiply by 100
     (0.4 → "40%", 0.375 → "37.5%", 0.17 → "17%")
   • Always output with % sign. ABV=0 → soft drink → leave blank.
2. custom_status — scan every column and text for T1 / T2. Check "STATUS" column.
3. refillable_status — STRICT:
   • "CAN" in Refil Status → bottle_or_can_type:"can", refillable_status:""
   • "BOTTLE" → bottle_or_can_type:"bottle", refillable_status:""
   • "REF" → refillable_status:"REF"
   • Blank → both fields blank. NEVER output "NRF" unless explicitly written.
4. supplier_reference — scan for P.Code, Ref, SKU, Item Code, Product Code.
5. unit_volume_ml — CONTENT column is in cl → multiply by 10 (70→700, 100→1000).
6. currency — "EURO" or "EURO " → "EUR".
7. Skip section header rows and blank rows. Extract incoterm/status from footers.

Text Chunk ({idx + 1}/{total}):
{chunk}
"""


# =============================================================================
# Helper: user-turn prompt for Excel batch rows
# =============================================================================
def _build_excel_user_prompt(data_rows: list, batch_start: int, batch_end: int,
                              total_rows: int, global_context: dict = None) -> str:
    context_block = ""
    if global_context:
        context_block = f"""
Global context from this file (apply to ALL products unless a row overrides it):
{json.dumps(global_context, indent=2)}

"""
    return f"""Extract products from these Excel rows ({batch_start + 1}–{batch_end} of {total_rows}).
Return ONLY a JSON object with a 'products' array. No explanations, no markdown.
{context_block}
⚠️ MANDATORY EXTRACTION RULES FOR THESE ROWS:

1. alcohol_percent — "ABV" column stores DECIMAL FRACTIONS. ALWAYS multiply by 100:
   0.4→"40%"  0.375→"37.5%"  0.17→"17%"  0.46→"46%"  0.463→"46.3%"
   0.3→"30%"  0.15→"15%"     0.04→"4%"   0.034→"3.4%" 0.055→"5.5%"
   0.33→"33%" 0.35→"35%"     0.38→"38%"  0.43→"43%"   0.45→"45%"
   0.414→"41.4%"  0.425→"42.5%"  0.31→"31%"  0.2→"20%"  0.16→"16%"
   ABV=0 → soft drink → alcohol_percent: "" (blank)

2. custom_status — "STATUS" column: extract "T2" or "T1" directly as-is.

3. refillable_status — "Refil Status" column DUAL-PURPOSE:
   "REF"    → refillable_status:"REF",   bottle_or_can_type:"bottle"
   "CAN"    → bottle_or_can_type:"can",  refillable_status:""
   "BOTTLE" → bottle_or_can_type:"bottle", refillable_status:""
   blank    → refillable_status:"",      bottle_or_can_type:""
   ⚠️ NEVER output "NRF" unless the source text literally says "NRF".

4. unit_volume_ml — "CONTENT" column is in CENTILITRES → multiply by 10:
   70→700  100→1000  35→350  50→500  20→200  75→750  150→1500  200→2000
   Exception: 500, 330, 275 in RTD/can context → treat as ml already.

5. currency — "EURO" or "EURO " (trailing space) → output "EUR".

6. supplier_reference — "P.Code" column → extract the code value.
   Also set product_reference to the same value.

7. quantity_case — "CASES" column → extract numeric value only.
   "5 FCL", "4 FCL", "1 FCL" etc. → quantity_case: null, add "fcl_quantity_not_extracted" to error_flags.

8. packaging — construct from units_per_case + unit_volume_ml:
   units_per_case=6, unit_volume_ml=700 → packaging:"6x700ml"

9. SKIP these rows entirely (do not create a product entry):
   • Row where PRODUCT cell contains only: "SPIRITS", "BEER", "RTDS",
     "MINERALS - NRB GLASS", "WINES", or any other section label
   • Rows where ALL cells are blank / empty string
   • Footer/note rows (contain: "All T2 EAD", "Ex Warehouse", "As at...",
     "All goods are in stock", "subject unsold")

10. ean_code — "Barcode Bottle" column → extract as string, "" if NaN.

Excel rows (JSON):
{json.dumps(data_rows, indent=2)}
"""


# =============================================================================
# Helper: extract global context from the full Excel dataframe
# =============================================================================
def _extract_excel_global_context(df: pd.DataFrame) -> dict:
    """
    Scan all rows for footer/header metadata:
    incoterm, supplier info, global custom_status, offer date.
    """
    context = {}
    all_text_values = []
    for val in df.values.flatten():
        s = str(val).strip()
        if s and s.lower() not in ("nan", "none", ""):
            all_text_values.append(s)
    full_text = " ".join(all_text_values)

    # Incoterm detection
    exw_match = re.search(
        r"ex\s+warehouse\s+([\w\s,]+?)(?:\.|,\s*Ireland|\s*$)",
        full_text, re.IGNORECASE
    )
    if exw_match:
        loc = exw_match.group(1).strip().rstrip(",.")
        # Check if "Ireland" follows
        ireland_match = re.search(
            r"ex\s+warehouse\s+([\w\s,]+?Ireland)", full_text, re.IGNORECASE)
        if ireland_match:
            loc = ireland_match.group(1).strip().rstrip(",.")
        context["incoterm"] = "EXW"
        context["location"] = loc
    else:
        inc_match = re.search(
            r"\b(EXW|FOB|CIF|DAP|DDP|FCA|CPT|CFR)\s+([\w\s,]+?)(?:\.|,|$)",
            full_text, re.IGNORECASE
        )
        if inc_match:
            context["incoterm"] = inc_match.group(1).upper()
            context["location"] = inc_match.group(2).strip().rstrip(",.")

    # Global T1/T2 default
    if re.search(r"\bAll\s+T2\b", full_text, re.IGNORECASE):
        context["custom_status_default"] = "T2"
    elif re.search(r"\bAll\s+T1\b", full_text, re.IGNORECASE):
        context["custom_status_default"] = "T1"

    # Offer date from header
    date_match = re.search(
        r"Offer\s+(\d{1,2}[./]\d{1,2}[./]\d{4})", full_text, re.IGNORECASE)
    if date_match:
        context["offer_date"] = date_match.group(1)

    logger.info(f"Extracted global Excel context: {context}")
    return context


async def extract_offer(text: str) -> dict:
    logger.info(f"extract_offer called with text length: {len(text)}")
    logger.debug(f"extract_offer text preview: {text[:200]}...")

    CHUNK_SIZE = 25000
    text_chunks = (
        [text[i:i + CHUNK_SIZE] for i in range(0, len(text), CHUNK_SIZE)]
        if len(text) > CHUNK_SIZE
        else [text]
    )
    logger.info(f"Split input text into {len(text_chunks)} chunk(s).")

    all_products = []

    for idx, chunk in enumerate(text_chunks):
        logger.info(f"Processing chunk {idx + 1} of {len(text_chunks)}...")
        try:
            response = await client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": MASTER_SYSTEM_PROMPT},
                    {"role": "user", "content": _build_text_user_prompt(chunk, idx, len(text_chunks))}
                ],
                response_format={"type": "json_object"},
                temperature=0.0,
                max_tokens=4096
            )
            content = response.choices[0].message.content
            logger.info(f"Response for chunk {idx + 1}, length: {len(content)}")

            result = json.loads(content)
            products = result.get('products', [])
            cleaned = [clean_product_data(p) for p in products]
            all_products.extend(cleaned)
            logger.info(f"Chunk {idx + 1} yielded {len(cleaned)} products.")

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error chunk {idx + 1}: {e}")
            continue
        except Exception as e:
            logger.error(f"Error chunk {idx + 1}: {e}")
            logger.error(traceback.format_exc())
            continue

    # Convert prices to EUR
    logger.info("Converting prices to EUR...")
    for product in all_products:
        currency = product.get('currency', "")
        if currency in ["", None, "EUR"]:
            product['price_per_unit_eur'] = product.get('price_per_unit')
            product['price_per_case_eur'] = product.get('price_per_case')
            continue
        exchange_rate = await get_exchange_rate_to_eur(currency)
        if product.get('price_per_unit') not in [None, "", 0]:
            product['price_per_unit_eur'] = convert_price_to_eur(
                product['price_per_unit'], currency, exchange_rate)
        if product.get('price_per_case') not in [None, "", 0]:
            product['price_per_case_eur'] = convert_price_to_eur(
                product['price_per_case'], currency, exchange_rate)

    logger.info(f"extract_offer completed. Total products: {len(all_products)}")
    return {"products": all_products}


async def extract_from_file(file_path: str, content_type: str) -> Dict[str, Any]:
    logger.info(f"extract_from_file: {file_path}, {content_type}")

    try:
        text_content = ""

        # ── EXCEL ──────────────────────────────────────────────────────────
        if content_type in [
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.ms-excel',
            'application/vnd.ms-excel.sheet.macroEnabled.12',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.macroEnabled.12'
        ] or file_path.lower().endswith(('.xlsx', '.xls', '.xlsm')):
            logger.info("Processing Excel file...")
            try:
                if not os.path.exists(file_path):
                    return {"error": f"File not found: {file_path}"}

                file_ext = os.path.splitext(file_path)[1].lower()
                logger.info(f"File size: {os.path.getsize(file_path)} bytes, ext: {file_ext}")

                try:
                    if file_ext == '.xls':
                        df = pd.read_excel(file_path, engine='xlrd', header=None)
                    else:
                        df = pd.read_excel(file_path, engine='openpyxl', header=None)
                except Exception as read_error:
                    logger.warning(f"Primary read failed: {read_error}. Trying fallback...")
                    df = pd.read_excel(file_path, header=None)

                logger.info(f"Excel loaded. Shape: {df.shape}")

                if df.empty:
                    return {"error": "Excel file is empty"}

                global_context = _extract_excel_global_context(df)
                total_rows = len(df)
                batch_size = 6
                all_extracted_products = []
                processed_row_count = 0

                for batch_start in range(0, total_rows, batch_size):
                    batch_end = min(batch_start + batch_size, total_rows)
                    batch_df = df.iloc[batch_start:batch_end]

                    logger.info(
                        f"Batch {batch_start // batch_size + 1}: "
                        f"rows {batch_start}–{batch_end - 1}")

                    data_rows = []
                    for idx, row in batch_df.iterrows():
                        row_dict = {
                            str(col): ("" if pd.isna(row[col]) else str(row[col]))
                            for col in batch_df.columns
                        }
                        data_rows.append(row_dict)

                    try:
                        response = await client.chat.completions.create(
                            model="gpt-4o",
                            messages=[
                                {
                                    "role": "system",
                                    "content": (
                                        MASTER_SYSTEM_PROMPT
                                        + "\n\nNOTE: Skip blank rows and section header rows "
                                        "silently — it is acceptable to return fewer products "
                                        "than input rows when some rows are headers or blank."
                                    )
                                },
                                {
                                    "role": "user",
                                    "content": _build_excel_user_prompt(
                                        data_rows, batch_start, batch_end,
                                        total_rows, global_context
                                    )
                                }
                            ],
                            response_format={"type": "json_object"},
                            temperature=0.0,
                            max_tokens=4096,
                            top_p=1.0,
                            frequency_penalty=0.0,
                            presence_penalty=0.0
                        )

                        content = response.choices[0].message.content
                        logger.info(f"Batch response length: {len(content)}")

                        # JSON repair if truncated
                        if not content.strip().endswith('}'):
                            logger.warning("JSON incomplete, attempting repair")
                            json_start = content.find('{')
                            if json_start != -1:
                                open_b = close_b = 0
                                for i, ch in enumerate(content[json_start:]):
                                    if ch == '{':
                                        open_b += 1
                                    elif ch == '}':
                                        close_b += 1
                                        if close_b == open_b:
                                            content = content[json_start:json_start + i + 1]
                                            break

                        result = json.loads(content)

                        if isinstance(result, dict) and 'products' in result:
                            batch_products = result['products']
                        elif isinstance(result, list):
                            batch_products = result
                        else:
                            batch_products = []

                        cleaned_batch = [clean_product_data(p) for p in batch_products]
                        all_extracted_products.extend(cleaned_batch)
                        processed_row_count += len(batch_df)

                    except json.JSONDecodeError as e:
                        logger.error(f"JSON error batch {batch_start // batch_size + 1}: {e}")
                        try:
                            matches = re.findall(r'\{.*\}', content, re.DOTALL)
                            for match in matches:
                                try:
                                    salvaged = json.loads(match)
                                    if isinstance(salvaged, dict) and 'products' in salvaged:
                                        cleaned_batch = [
                                            clean_product_data(p)
                                            for p in salvaged['products']
                                        ]
                                        all_extracted_products.extend(cleaned_batch)
                                        logger.warning(
                                            f"Salvaged {len(cleaned_batch)} products via regex")
                                        break
                                except Exception:
                                    continue
                        except Exception as se:
                            logger.error(f"Salvage failed: {se}")
                        processed_row_count += len(batch_df)

                    except Exception as e:
                        logger.error(f"Batch error: {e}")
                        processed_row_count += len(batch_df)

                logger.info(
                    f"Extracted {len(all_extracted_products)} products "
                    f"from {processed_row_count} rows")

                if all_extracted_products:
                    logger.info("Converting prices to EUR for Excel products...")
                    for product in all_extracted_products:
                        currency = product.get('currency', "")
                        if currency in ["", None, "EUR"]:
                            product['price_per_unit_eur'] = product.get('price_per_unit')
                            product['price_per_case_eur'] = product.get('price_per_case')
                            continue
                        exchange_rate = await get_exchange_rate_to_eur(currency)
                        if product.get('price_per_unit') not in [None, "", 0]:
                            product['price_per_unit_eur'] = convert_price_to_eur(
                                product['price_per_unit'], currency, exchange_rate)
                        if product.get('price_per_case') not in [None, "", 0]:
                            product['price_per_case_eur'] = convert_price_to_eur(
                                product['price_per_case'], currency, exchange_rate)

                    return {
                        'products': all_extracted_products,
                        'total_products': len(all_extracted_products),
                        'file_type': 'excel',
                        'processed_in_batches': True,
                        'batches_processed': (total_rows + batch_size - 1) // batch_size,
                        'original_rows': total_rows
                    }
                else:
                    logger.warning("No products extracted, trying text fallback...")
                    simplified_rows = []
                    for i in range(min(10, len(df))):
                        row = df.iloc[i]
                        row_dict = {
                            str(col): ("" if pd.isna(row[col]) else str(row[col]))
                            for col in df.columns
                        }
                        simplified_rows.append(row_dict)
                    text_content = (
                        f"Excel with {total_rows} rows. Sample:\n"
                        f"{json.dumps(simplified_rows, indent=2)}"
                    )
                    fallback = await extract_offer(text_content)
                    if isinstance(fallback, dict) and 'products' in fallback:
                        for product in fallback['products']:
                            if product.get('currency') not in ["", None, "EUR"]:
                                er = await get_exchange_rate_to_eur(product.get('currency'))
                                if product.get('price_per_unit') not in [None, "", 0]:
                                    product['price_per_unit_eur'] = convert_price_to_eur(
                                        product['price_per_unit'], product.get('currency'), er)
                                if product.get('price_per_case') not in [None, "", 0]:
                                    product['price_per_case_eur'] = convert_price_to_eur(
                                        product['price_per_case'], product.get('currency'), er)
                    return fallback

            except Exception as e:
                logger.error(f"Excel error: {e}")
                logger.error(traceback.format_exc())
                return {"error": f"Excel read error: {str(e)}"}

        # ── PDF ────────────────────────────────────────────────────────────
        elif content_type == 'application/pdf':
            logger.info("Processing PDF file...")
            try:
                import PyPDF2
                with open(file_path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    text_content = "".join(page.extract_text() for page in reader.pages)
                logger.info(f"PDF text length: {len(text_content)}")
            except ImportError:
                return {"error": "PyPDF2 not installed"}
            except Exception as e:
                logger.error(f"PDF error: {e}")
                return {"error": f"PDF read error: {str(e)}"}

        # ── IMAGE ──────────────────────────────────────────────────────────
        elif 'image' in content_type:
            logger.info("Processing image file...")
            try:
                with open(file_path, "rb") as f:
                    base64_image = base64.b64encode(f.read()).decode('utf-8')

                response = await client.chat.completions.create(
                    model="gpt-4o",
                    messages=[
                        {"role": "system", "content": MASTER_SYSTEM_PROMPT},
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": (
                                        "Extract all commercial alcohol/beverage offers from this image.\n"
                                        "Return ONLY a JSON object {'products': [...]}.\n\n"
                                        "Critical reminders:\n"
                                        "1. alcohol_percent: ABV < 1.0 → multiply by 100 → '40%'.\n"
                                        "2. refillable_status: CAN/BOTTLE → bottle_or_can_type only. "
                                        "NEVER output NRF unless explicitly written.\n"
                                        "3. custom_status: extract T1 or T2 from any column/text.\n"
                                        "4. Blank fields → '' or null. Never 'Not Found' or 0."
                                    )
                                },
                                {
                                    "type": "image_url",
                                    "image_url": f"data:{content_type};base64,{base64_image}",
                                },
                            ],
                        }
                    ],
                    max_tokens=4096,
                )
                logger.info("Image processed")
                raw = response.choices[0].message.content
                result = await extract_offer(raw)
                if isinstance(result, dict) and 'products' in result:
                    for product in result['products']:
                        if product.get('currency') not in ["", None, "EUR"]:
                            er = await get_exchange_rate_to_eur(product.get('currency'))
                            if product.get('price_per_unit') not in [None, "", 0]:
                                product['price_per_unit_eur'] = convert_price_to_eur(
                                    product['price_per_unit'], product.get('currency'), er)
                            if product.get('price_per_case') not in [None, "", 0]:
                                product['price_per_case_eur'] = convert_price_to_eur(
                                    product['price_per_case'], product.get('currency'), er)
                return result

            except Exception as e:
                logger.error(f"Image error: {e}")
                logger.error(traceback.format_exc())
                return {"error": f"Image processing error: {str(e)}"}

        # ── TEXT / OTHER ───────────────────────────────────────────────────
        else:
            logger.info(f"Processing text file, content_type: {content_type}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    text_content = f.read()
            except UnicodeDecodeError:
                try:
                    with open(file_path, 'r', encoding='latin-1') as f:
                        text_content = f.read()
                except Exception as e:
                    return {"error": f"Text file read error: {str(e)}"}
            except Exception as e:
                return {"error": f"Text file read error: {str(e)}"}

        if text_content:
            return await extract_offer(text_content)
        else:
            return {"error": "No content extracted from file"}

    except Exception as e:
        logger.error(f"extract_from_file error: {e}")
        logger.error(traceback.format_exc())
        return {"error": f"General extraction error: {str(e)}"}


# =============================================================================
# clean_product_data
# Lightweight schema sanitiser only — NO business logic transformation.
# The AI outputs correct values. This function only:
#   1. Ensures all schema fields exist
#   2. Casts numeric fields to float/None
#   3. Removes sentinel strings → proper blanks
#   4. Safety-net: decimal ABV → "XX%"
#   5. Safety-net: refillable_status whitelist
#   6. Safety-net: currency "EURO" → "EUR"
# =============================================================================
def clean_product_data(product: dict) -> dict:
    """Lightweight schema enforcer. No business logic — AI handles that."""

    schema_defaults = {
        'uid': "",
        'product_key': "",
        'processing_version': "",
        'brand': "",
        'product_name': "",
        'product_reference': "",
        'category': "",
        'sub_category': "",
        'origin_country': "",
        'vintage': "",
        'alcohol_percent': "",
        'packaging': "",
        'unit_volume_ml': None,
        'units_per_case': None,
        'cases_per_pallet': None,
        'quantity_case': None,
        'bottle_or_can_type': "",
        'price_per_unit': None,
        'price_per_case': None,
        'currency': "",
        'price_per_unit_eur': None,
        'price_per_case_eur': None,
        'incoterm': "",
        'location': "",
        'min_order_quantity_case': None,
        'port': "",
        'lead_time': "",
        'supplier_name': "",
        'supplier_reference': "",
        'supplier_country': "",
        'offer_date': "",
        'valid_until': "",
        'date_received': "",
        'source_channel': "",
        'source_filename': "",
        'source_message_id': "",
        'confidence_score': 0.0,
        'error_flags': [],
        'needs_manual_review': False,
        'best_before_date': "",
        'label_language': "",
        'ean_code': "",
        'gift_box': "",
        'refillable_status': "",
        'custom_status': "",
        'moq_cases': None,
    }

    SENTINELS = {
        None, "Not Found", "not found", "NOT FOUND", "null", "NULL",
        "N/A", "n/a", "na", "NA", "none", "None", "NONE", ""
    }

    NUMERIC_FIELDS = {
        'unit_volume_ml', 'units_per_case', 'cases_per_pallet', 'quantity_case',
        'price_per_unit', 'price_per_case', 'price_per_unit_eur', 'price_per_case_eur',
        'min_order_quantity_case', 'moq_cases',
    }

    NUMERIC_NEVER_ZERO = {
        'unit_volume_ml', 'units_per_case', 'cases_per_pallet', 'quantity_case',
        'min_order_quantity_case', 'moq_cases',
    }

    cleaned = {}

    for field, default in schema_defaults.items():
        raw = product.get(field, default)

        # ── Numeric fields ────────────────────────────────────────────────
        if field in NUMERIC_FIELDS:
            if raw in SENTINELS or raw == 0 or raw == "0":
                cleaned[field] = None
            else:
                try:
                    fval = float(str(raw).replace(',', '.'))
                    if field in NUMERIC_NEVER_ZERO and fval == 0:
                        cleaned[field] = None
                    else:
                        cleaned[field] = fval
                except (ValueError, TypeError):
                    cleaned[field] = None

        # ── alcohol_percent — preserve AI output + safety-net conversion ──
        elif field == 'alcohol_percent':
            if raw in SENTINELS or raw == 0 or raw == "0" or raw == "0%":
                cleaned[field] = ""
            elif isinstance(raw, str) and raw.strip().endswith('%'):
                # AI output correct format — keep as-is
                cleaned[field] = raw.strip()
            elif isinstance(raw, (int, float)):
                fval = float(raw)
                if fval == 0:
                    cleaned[field] = ""
                elif fval < 1.0:
                    # Safety net: decimal fraction slipped through
                    pct = round(fval * 100, 2)
                    cleaned[field] = f"{int(pct)}%" if pct == int(pct) else f"{pct}%"
                elif fval.is_integer():
                    cleaned[field] = f"{int(fval)}%"
                else:
                    cleaned[field] = f"{fval}%"
            elif isinstance(raw, str):
                s = raw.strip()
                if not s:
                    cleaned[field] = ""
                else:
                    try:
                        fval = float(s.replace('%', '').replace(',', '.'))
                        if fval == 0:
                            cleaned[field] = ""
                        elif fval < 1.0:
                            pct = round(fval * 100, 2)
                            cleaned[field] = f"{int(pct)}%" if pct == int(pct) else f"{pct}%"
                        elif fval.is_integer():
                            cleaned[field] = f"{int(fval)}%"
                        else:
                            cleaned[field] = f"{fval}%"
                    except (ValueError, TypeError):
                        cleaned[field] = ""
            else:
                cleaned[field] = ""

        # ── refillable_status — strict whitelist safety net ───────────────
        elif field == 'refillable_status':
            if raw in SENTINELS:
                cleaned[field] = ""
            else:
                val = str(raw).strip().upper()
                if val in ("RF", "REF", "REFILLABLE"):
                    cleaned[field] = "REF"
                elif val in ("NRF", "NON-REFILLABLE", "NON REFILLABLE"):
                    cleaned[field] = "NRF"
                else:
                    # CAN, BOTTLE, or anything else → not a refillable_status value
                    cleaned[field] = ""

        # ── currency — normalise EURO → EUR safety net ────────────────────
        elif field == 'currency':
            if raw in SENTINELS:
                cleaned[field] = ""
            else:
                val = str(raw).strip().upper()
                if val in ("EURO", "EUROS", "€"):
                    cleaned[field] = "EUR"
                else:
                    cleaned[field] = val

        # ── List fields ───────────────────────────────────────────────────
        elif isinstance(default, list):
            cleaned[field] = raw if isinstance(raw, list) else []

        # ── Boolean fields ────────────────────────────────────────────────
        elif isinstance(default, bool):
            cleaned[field] = bool(raw) if raw not in SENTINELS else False

        # ── Float (confidence_score) ──────────────────────────────────────
        elif isinstance(default, float):
            try:
                cleaned[field] = float(raw)
            except (ValueError, TypeError):
                cleaned[field] = default

        # ── All other string fields ───────────────────────────────────────
        else:
            cleaned[field] = "" if raw in SENTINELS else str(raw)

    # Auto-generate product_key if missing
    if not cleaned.get('product_key') and cleaned.get('product_name'):
        cleaned['product_key'] = (
            str(cleaned['product_name'])
            .replace(' ', '_').replace('/', '_')
            .replace('&', '_').replace('.', '').replace("'", "")
            .upper()
        )

    return cleaned


def parse_buffer_data(buffer_data: dict) -> bytes:
    logger.debug(f"parse_buffer_data called with buffer_data type: {type(buffer_data)}")

    if isinstance(buffer_data, dict) and buffer_data.get('type') == 'Buffer':
        try:
            data_bytes = bytes(buffer_data['data'])
            logger.debug(f"Parsed Buffer type, length: {len(data_bytes)} bytes")
            return data_bytes
        except Exception as e:
            logger.error(f"Error parsing Buffer type: {e}")
            return b''
    elif isinstance(buffer_data, dict) and 'data' in buffer_data:
        try:
            if isinstance(buffer_data['data'], str):
                data_bytes = base64.b64decode(buffer_data['data'])
                logger.debug(f"Parsed base64 string, length: {len(data_bytes)} bytes")
                return data_bytes
            elif isinstance(buffer_data['data'], list):
                data_bytes = bytes(buffer_data['data'])
                logger.debug(f"Parsed list data, length: {len(data_bytes)} bytes")
                return data_bytes
            else:
                logger.warning(
                    f"Unexpected data type in buffer_data['data']: {type(buffer_data['data'])}")
                return b''
        except Exception as e:
            logger.error(f"Error parsing buffer_data with 'data' key: {e}")
            return b''
    elif isinstance(buffer_data, str):
        try:
            data_bytes = base64.b64decode(buffer_data)
            logger.debug(f"Parsed base64 string directly, length: {len(data_bytes)} bytes")
            return data_bytes
        except Exception as e:
            logger.error(f"Error parsing base64 string: {e}")
            return b''
    else:
        logger.warning(f"Unexpected buffer_data type: {type(buffer_data)}")
        return b''