import aiohttp
import asyncio
import base64
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from Models.models import CompanyInfo, DirectorInfo, FilingInfo, LegalInfo, BusinessProfile
from typing import Dict, Optional, Any
from pathlib import Path
from dataclasses import asdict


SIC_CODES = Path("Company_House/sic_codes/sic_codes_grouped.json")

class CompanyHouseAPI:
    __search_url = "https://api.company-information.service.gov.uk/advanced-search/companies"
    __get_company_url = "https://api.company-information.service.gov.uk/company"
    __base_url = "https://api.company-information.service.gov.uk"
    __sic_data = []

    def __init__(self):
        with open(SIC_CODES, "r") as file:
            self.__sic_data = json.loads(file.read())

    def age_str(self, dob: dict):
        dt = datetime(dob.get("year", ""), dob.get("month", ""), 1)
        today = datetime.today()
        diff = relativedelta(today, dt).years
        return diff

    def is_last_month(self, date_string: str):
        dt = datetime.strptime(date_string, "%Y-%m-%d")
        today = datetime.today()

        if today.month == 1:
            last_month = 12
            last_month_year = today.year - 1
        else:
            last_month = today.month - 1
            last_month_year = today.year
        
        return dt.year == last_month_year and dt.month == last_month

    def months_diff(self, date_string: str):
        dt = datetime.strptime(date_string, "%Y-%m-%d")
        today = datetime.today()
        delta = relativedelta(today, dt)
        total_months = delta.years * 12 + delta.months
        return total_months
    
    def get_sic_description(self, sic_code):
        for k, v in self.__sic_data.items():
            for entry in v:
                if entry["sic_code"] == sic_code:
                    return {
                        "Sector": k,
                        "Sub-sector": entry["description"],
                        "Sic Code": sic_code
                    }
        return None

    async def search_company(self, session: aiohttp.ClientSession, headers: dict, **kwargs) -> dict:
        try:
            async with session.get(self.__search_url, headers=headers, params=kwargs) as resp:
                if resp.status == 200:
                    json_resp = await resp.json()
                    return json_resp.get("top_hit", {})
                else:
                    print(f"❌ Error {resp.status}: {await resp.text()}")
        except Exception as e:
            print(f"Exception occurred: {e}")

    async def get_company_details(self, session: aiohttp.ClientSession, headers: dict, company_number: str):
        try:
            url = f"{self.__get_company_url}/{company_number}"
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    json_resp = await resp.json()
                    return json_resp
                else:
                    print(f"❌ Error {resp.status}: {await resp.text()}")
        except Exception as e:
            print(f"Exception occurred: {e}")

    async def fetch_link(self, session: aiohttp.ClientSession, url_link: str, headers: dict):
        try:
            url = f"{self.__base_url}{url_link}"
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    json_resp = await resp.json()
                    return json_resp
                else:
                    print(f"❌ Error {resp.status}: {await resp.text()}")
        except Exception as e:
            print(f"Exception occurred: {e}")
    
    async def run(self, headers: dict, params: dict):
        retVal = None
        async with aiohttp.ClientSession() as session:
            search_result = await self.search_company(session, headers, **params)
            if search_result:
                company_number = search_result.get("company_number", "")
                company_details = await self.get_company_details(session, headers, company_number)
                company_info = CompanyInfo(
                    company_details.get("company_name", ""),
                    company_details.get("company_number", ""),
                    company_details.get("registered_office_address", {}).get("locality", ""),
                    company_details.get("registered_office_address", {}),
                    company_details.get("date_of_creation", ""),
                    "Yes" if company_details.get("company_status") == "active" else "No",
                    "Yes" if company_details.get("company_status") == "active" else "No",
                    [self.get_sic_description(sid_codes) for sid_codes in company_details.get("sic_codes", [])],
                    "No"
                    )
                filing_history = company_details.get("links", {}).get("filing_history", "")
                filing_details = await self.fetch_link(session, filing_history, headers)
                filing_info = FilingInfo(
                    latest_account_filing_date=filing_details.get("items")[0].get("date"),
                    account_filing_in_past_month="Yes" if self.is_last_month(filing_details.get("items")[0].get("date")) else "No",
                    months_since_last_filing=self.months_diff(filing_details.get("items")[0].get("date"))
                )
                officers = company_details.get("links", {}).get("officers", "")
                director_details = await self.fetch_link(session, officers, headers)
                director_info = DirectorInfo(
                    number_of_directors=director_details.get("active_count", ""),
                    names_of_other_directors=[
                        _.get("name", "")
                        for _ in director_details.get("items", [])
                        if _.get("officer_role") == "director" and _.get("resigned_on", "") == ""
                    ],
                    director_age_years=[
                        {_.get("name", ""): self.age_str(_.get("date_of_birth", {}))}
                        for _ in director_details.get("items", [])
                        if _.get("officer_role") == "director" and _.get("resigned_on", "") == ""
                    ]
                )
                charges = company_details.get("links", {}).get("charges", "")
                if charges:
                    charges_details = await self.fetch_link(session, charges, headers)
                    legal_info = LegalInfo(
                        has_debentures_or_charges="Yes" if charges_details.get("total_count") > 0 else "No",
                        debentures_status="Has Outstanding" if charges_details.get("part_satisfied_count") > 0 else "All Satisfied",
                        outstanding_count=charges_details.get("part_satisfied_count"),
                        satisfied_count=charges_details.get("satisfied_count")
                    )
                else:
                    legal_info = None
                retVal = BusinessProfile(
                    company_info=company_info,
                    director_info=director_info,
                    filing_info=filing_info,
                    legal_info=legal_info
                )
        return retVal


async def run_business_profiling(logger, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    COMPANY_HOUSE_API_KEY = os.environ.get("COMPANY_HOUSE_API_KEY")
    auth = base64.b64encode(f"{COMPANY_HOUSE_API_KEY}:".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth}",
        "Accept": "application/json"
    }

    retVal = []

    for k, v in data.items():
        if k == "companies":
            for company in v:
                new_company = CompanyHouseAPI()
                retval = await new_company.run(headers, {"company_name_includes": company})
                retval = asdict(retval)
                retVal.append(retval)
            break
    return retVal

async def main():
    query_params = {
        "company_name_includes": "M & W Property Services Limited",
        "company_status": "active",
        "location": "Aylesbury"
    }
    q = {'company_name_includes': 'THOMAS BRADLEY FS LIMITED'}

    API_KEY = "bb7f9e56-275c-4b33-a1d5-50d0e6e1c35d"
    auth = base64.b64encode(f"{API_KEY}:".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth}",
        "Accept": "application/json"
    }

    new_company = CompanyHouseAPI()
    retval = await new_company.run(headers, q)

if __name__ == "__main__":
    asyncio.run(main())
