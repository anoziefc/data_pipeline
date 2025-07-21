import requests
from bs4 import BeautifulSoup
import json

URL = "https://resources.companieshouse.gov.uk/sic/"

def scrape_grouped_sic_codes():
    response = requests.get(URL)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch page: {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")

    if not table:
        raise Exception("SIC table not found on the page.")

    grouped_data = {}
    current_section = "Uncategorized"

    for row in table.find_all("tr"):
        cols = row.find_all(["th", "td"])

        if len(cols) == 1 and cols[0].name == "th":
            current_section = cols[0].text.strip()
            grouped_data[current_section] = []

        elif len(cols) == 2:
            sic_code = cols[0].text.strip()
            description = cols[1].text.strip()
            grouped_data.setdefault(current_section, []).append({
                "sic_code": sic_code,
                "description": description
            })

    return grouped_data

sic_grouped = scrape_grouped_sic_codes()

with open("sic_codes_grouped.json", "w", encoding="utf-8") as f:
    json.dump(sic_grouped, f, ensure_ascii=False, indent=2)

print("Grouped SIC codes saved to sic_codes_grouped.json")
