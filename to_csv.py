import pandas as pd
import json
from datetime import datetime
from dateutil import parser
import re


def months_active(active_since_str, date_format="%Y-%m-%d"):
    active_since = parser.isoparse(active_since_str)
    today = datetime.now()

    total_months = (today.year - active_since.year) * 12 + (today.month - active_since.month)

    if today.day < active_since.day:
        total_months -= 1
    return total_months

def convert_date_format(date_str):
    if not isinstance(date_str, str):
        return None
    date_obj = parser.parse(date_str)
    return date_obj.strftime("%d-%m-%Y")

def convert_iso_to_ddmmyyyy(iso_str):
    date_obj = parser.isoparse(iso_str)
    return date_obj.strftime("%d-%m-%Y")

df_main = pd.read_csv("new_directors_output.csv")

# === Load BIDSTATS ===
with open("tz/data/bidstats/bidstats.json", "r", encoding="utf-8") as f:
    bidstats_raw = json.load(f)

# Build lookup: supplier name → bidstats record
bidstats_lookup = {}
for record in bidstats_raw:
    supplier_list = record.get("suppliers", [])
    for supplier in supplier_list:
        supplier_name = supplier.get("name", "").strip().lower()
        if supplier_name:
            bidstats_lookup[supplier_name] = record

# === Load TRUST PILOT DEFAULT ===
with open("tz/NA/trust_pilot.json", "r", encoding="utf-8") as f:
    tax_default_raw = json.load(f)

# Build lookup: name → trust_pilot default record
trust_pilot_lookup = {}
for record in tax_default_raw:
    results = record.get("results", [])
    if results:
        company_names = [company_obj.get("company_name", "").strip().lower() for company_obj in results if company_obj.get("company_name", None)]
        if company_names:
            for name in company_names:
                trust_pilot_lookup[name] = record

# === Load TAX DEFAULT ===
with open("tz/data/tax_defaulters/tax_defaulters.json", "r", encoding="utf-8") as f:
    tax_default_raw = json.load(f)

# Build lookup: name → tax default record
tax_lookup = {
    entry["Name"].strip().lower(): entry
    for entry in tax_default_raw
    if "Name" in entry
}

def format_amount(amount_str):
    if not amount_str or not isinstance(amount_str, str):
        return None

    amount_str = amount_str.lower().strip()

    multiplier = 1
    if amount_str.endswith('k'):
        multiplier = 1_000
        amount_str = amount_str[:-1]
    elif amount_str.endswith('m'):
        multiplier = 1_000_000
        amount_str = amount_str[:-1]

    amount_str = amount_str.replace(',', '.')

    cleaned = []
    dot_found = False
    for char in amount_str:
        if char.isdigit():
            cleaned.append(char)
        elif char == '.' and not dot_found:
            cleaned.append('.')
            dot_found = True

    cleaned_amount = ''.join(cleaned)

    try:
        return float(cleaned_amount) * multiplier if cleaned_amount else None
    except ValueError:
        return None

# def normalize_company_name(name):
#     name = name.lower().strip()
#     name = re.sub(r'\bltd\b|\blimited\b|\bplc\b|\binc\b|\bcorp\b', '', name)  # remove common suffixes
#     name = re.sub(r'[^a-z0-9]', '', name)  # remove all non-alphanumeric characters
#     return name

def normalize_company_name(name):
    name = name.lower().strip()
    # name = re.sub(r'\(.*?\)', '', name)
    name = re.sub(r'\s*\(.*?\)\s*', ' ', name)
    name = re.sub(r'\b(ltd|limited|plc|inc|corp|llc|gmbh)\b', '', name)
    # name = re.sub(r'\b(ltd|limited|plc|inc|corp|llc|gmbh)\b', '', name)
    name = re.sub(r'[^\w\s]', '', name)
    # name = re.sub(r'[^\w\s]', '', name)
    # name = re.sub(r'[^a-z0-9]', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()
    # return name



normalized_bidstats_lookup = {
    normalize_company_name(name): record
    for name, record in bidstats_lookup.items()
}

normalized_tax_lookup = {
    normalize_company_name(name): record
    for name, record in tax_lookup.items()
}

normalized_trust_pilot_lookup = {
    normalize_company_name(name): record
    for name, record in trust_pilot_lookup.items()
}

# === Enrichment function ===
def enrich_row(row):
    source = str(row.get("Data Source", "")).strip().lower()
    company = str(row.get("Company Name", "")).strip().lower()
    normalized_company = normalize_company_name(company)
    person = str(row.get("Name", "")).strip().lower()

    if source == "bidstats":
        record = normalized_bidstats_lookup.get(normalized_company)
        awarded_date = record.get("awarded_at") if record else None
        new_contract_awarded_date = convert_date_format(awarded_date) if awarded_date else None
        amount_awarded = record.get("formatted_value") if record else None
        return pd.Series({
            "Has Given TrustPilot Review": "No",
            "New Contract Awarded": record.get("is_awarded") if record else None,
            "New Contract Awarded Date": new_contract_awarded_date,
            "New Contract Awarded Summary": record.get("short_body") if record else None,
            "New Contract Awarded Amount": format_amount(amount_awarded) if amount_awarded else None
        })

    elif source == "tax default":
        record = normalized_tax_lookup.get(normalized_company)
        return pd.Series({
            "Has Given TrustPilot Review": "No",
            "Period of default": record.get("Period of   default") if record else None,
            "Address (Tax Default)": record.get("Address") if record else None,
            "Tax/Penalty Amount": record.get("Total amount   of tax/duty on which penalties are based and total amount of penalties   charged") if record else None
        })
    
    elif source == "trust pilot":
        record = normalized_trust_pilot_lookup.get(normalized_company)
        review = record.get("review", None) if record else None
        reviewer = record.get("reviewer", None) if record else None
        review_date = review.get("review_date") if review else None
        useful_date_format = convert_iso_to_ddmmyyyy(review_date) if review_date else None
        months_since_review = months_active(review_date) if review_date else None
        return pd.Series({
            "Has Given TrustPilot Review": "Yes" if review else "No",
            "TrustPilot Review Rating": review.get("star_rating", "") if review else None,
            "TrustPilot Review Text": review.get("reviewer_text") if review else None,
            "TrustPilot Review Date": useful_date_format,
            "TrustPilot Review URL": review.get("review_url") if review else None,
            "Reviewer Profile URL": reviewer.get("reviewer_profile_url") if review else None,
            "Months since TrustPilot Review": months_since_review
        })

    return pd.Series()

# === Apply enrichment and merge ===
enriched = df_main.apply(enrich_row, axis=1)
df_final = pd.concat([df_main, enriched], axis=1)
df_final = df_final.drop_duplicates()

# === Save output ===
df_final.to_csv("new_enriched_output.csv", index=False)

# if __name__ == "__main__":
    
#     # === Load BIDSTATS ===
#     with open("tz/data/bidstats/bidstats.json", "r", encoding="utf-8") as f:
#         bidstats_raw = json.load(f)

#     bidstats_lookup = {}
#     for record in bidstats_raw:
#         supplier_list = record.get("suppliers", [])
#         if supplier_list:
#             supplier_name = supplier_list[0].get("name", "").strip().lower()
#             if supplier_name:
#                 bidstats_lookup[supplier_name] = record
    
#     for record in bidstats_lookup.values():
#         awarded_date = record.get("awarded_at") if record else None
#         new_contract_awarded_date = convert_date_format(awarded_date) if awarded_date else None
#         break
