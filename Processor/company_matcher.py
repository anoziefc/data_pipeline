import re
import json
from typing import List, Dict
from pathlib import Path


SUFFIXES = ['ltd', 'limited', 'plc', 'llp', 'inc', 'corp', 'co', 'services']

def normalize_company_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r'[^\w\s]', ' ', name)  # Remove punctuation
    name = re.sub(r'\b(?:' + '|'.join(SUFFIXES) + r')\b', '', name)  # Remove suffixes
    name = re.sub(r'\s+', ' ', name).strip()  # Normalize whitespace
    return name

def match_companies(dataset1: List[List[Dict]], dataset2: List[Dict]) -> List[Dict]:
    normalized_map = {}
    for company_group in dataset1:
        for company_record in company_group:
            company_name = company_record.get('company_info', {}).get('company_name')
            if company_name:
                normalized_name = normalize_company_name(company_name)
                normalized_map[normalized_name] = company_record

    matched_data = []

    for person in dataset2:
        source = person.get("source")
        all_companies = person.get("companies", [])
        matched_names = []
        matched_records = []

        for company_name in all_companies:
            normalized = normalize_company_name(company_name)
            if normalized in normalized_map:
                matched_names.append(company_name)
                matched_records.append(normalized_map[normalized])

        fn = person.get("full_name", None)
        if fn:
            matched_data.append({
                "full_name": fn,
                "all_companies": all_companies,
                "matched_company_names": matched_names,
                "matched_company_records": matched_records,
                "source": source
            })
        else:
            matched_data.append({
                "all_companies": all_companies,
                "matched_company_names": matched_names,
                "matched_company_records": matched_records,
                "source": source
            })

    return matched_data
