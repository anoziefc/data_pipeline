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
from aiolimiter import AsyncLimiter


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
                    error_text = await resp.text()
                    raise aiohttp.ClientResponseError(
                        status=resp.status,
                        message=f"Company House API returned {resp.status}: {error_text}",
                        request_info=resp.request_info,
                        history=resp.history
                    )
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
                    error_text = await resp.text()
                    raise aiohttp.ClientResponseError(
                        status=resp.status,
                        message=f"Company House API returned {resp.status}: {error_text}",
                        request_info=resp.request_info,
                        history=resp.history
                    )
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
                    error_text = await resp.text()
                    raise aiohttp.ClientResponseError(
                        status=resp.status,
                        message=f"Company House API returned {resp.status}: {error_text}",
                        request_info=resp.request_info,
                        history=resp.history
                    )
        except Exception as e:
            print(f"Exception occurred: {e}")

    async def run(self, headers: dict, params: dict) -> BusinessProfile:
        async with aiohttp.ClientSession() as session:
            try:
                search_result = await self.search_company(session, headers, **params)
                if not search_result:
                    raise ValueError("No search result found.")

                company_number = search_result.get("company_number")
                if not company_number:
                    raise ValueError("Company number missing in search result.")

                company_details = await self.get_company_details(session, headers, company_number)
                if not company_details:
                    raise ValueError("Company details not found.")

                company_info = CompanyInfo(
                    company_name=company_details.get("company_name", ""),
                    company_number=company_details.get("company_number", ""),
                    uk_city_location=company_details.get("registered_office_address", {}).get("locality", ""),
                    registered_address=company_details.get("registered_office_address", {}),
                    active_since_date=company_details.get("date_of_creation", ""),
                    currently_active="Yes" if company_details.get("company_status") == "active" else "No",
                    is_the_company_active="Yes" if company_details.get("company_status") == "active" else "No",
                    industry_of_the_company_from_sic=[self.get_sic_description(code) for code in company_details.get("sic_codes", [])],
                    vat_registered="No"
                )

                filing_info = FilingInfo()
                filing_history_link = company_details.get("links", {}).get("filing_history")
                if filing_history_link:
                    filing_details = await self.fetch_link(session, filing_history_link, headers)
                    items = filing_details.get("items", [])
                    if items:
                        date_str = items[0].get("date")
                        if date_str:
                            filing_info.latest_account_filing_date = date_str
                            filing_info.account_filing_in_past_month = "Yes" if self.is_last_month(date_str) else "No"
                            filing_info.months_since_last_filing = self.months_diff(date_str)

                director_info = DirectorInfo()
                officers_link = company_details.get("links", {}).get("officers")
                if officers_link:
                    director_details = await self.fetch_link(session, officers_link, headers)
                    if director_details:
                        director_info.number_of_directors = director_details.get("active_count", "")
                        director_info.names_of_other_directors = [
                            d.get("name", "")
                            for d in director_details.get("items", [])
                            if d.get("officer_role") == "director" and not d.get("resigned_on")
                        ]
                        director_info.director_age_years = [
                            {d.get("name", ""): self.age_str(d.get("date_of_birth", {}))}
                            for d in director_details.get("items", [])
                            if d.get("officer_role") == "director" and not d.get("resigned_on")
                        ]

                legal_info = None
                charges_link = company_details.get("links", {}).get("charges")
                if charges_link:
                    charges_details = await self.fetch_link(session, charges_link, headers)
                    if charges_details:
                        legal_info = LegalInfo(
                            has_debentures_or_charges="Yes" if charges_details.get("total_count", 0) > 0 else "No",
                            debentures_status="Has Outstanding" if charges_details.get("part_satisfied_count", 0) > 0 else "All Satisfied",
                            outstanding_count=charges_details.get("part_satisfied_count", 0),
                            satisfied_count=charges_details.get("satisfied_count", 0),
                        )

                return BusinessProfile(
                    company_info=company_info,
                    director_info=director_info,
                    filing_info=filing_info,
                    legal_info=legal_info
                )

            except Exception as e:
                print(f"Exception in CompanyHouseAPI.run(): {e}")

                return BusinessProfile(
                    company_info=CompanyInfo(),
                    director_info=DirectorInfo(),
                    filing_info=FilingInfo(),
                    legal_info=None
                )

async def run_business_profiling(logger, data: Dict[str, Any], limiter: Optional[AsyncLimiter] = None) -> Optional[Dict[str, Any]]:
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
                if limiter:
                    async with limiter:
                        retval = await new_company.run(headers, {"company_name_includes": company})
                else:
                    retval = await new_company.run(headers, {"company_name_includes": company})
                retval = asdict(retval)
                retVal.append(retval)
            break
    return retVal
