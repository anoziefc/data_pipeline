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


if __name__ == "__main__":
    path1 = Path("data/result.json")
    path2 = Path("data/data.json")
    ret = Path("data/matched.json")

    with open(path1, "r") as p1:
        dataset1 = json.loads(p1.read())
    
    with open(path2, "r") as p2:
        dataset2 = json.loads(p2.read())

    matched = match_companies(dataset1, dataset2)
    with open(ret, "w") as f:
        json.dump(matched, f, indent=4)
