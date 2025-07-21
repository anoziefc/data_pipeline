import json
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime


def months_active(active_since_str, date_format="%Y-%m-%d"):
    active_since = datetime.strptime(active_since_str, date_format)
    today = datetime.today()

    total_months = (today.year - active_since.year) * 12 + (today.month - active_since.month)

    if today.day < active_since.day:
        total_months -= 1
    return total_months

def process_per_director(record):
    all_data = []
    full_name = record.get("full_name", "")
    matched_company_records = record.get("matched_company_records", [])
    companies_count = len(matched_company_records)
    all_companies = record.get("all_companies")
    source = record.get("source")
    if companies_count >= 1:
        for company in matched_company_records:
            company_info = company.get("company_info", {})
            director_info = company.get("director_info", {})
            filing_info = company.get("filing_info", {})
            legal_info = company.get("legal_info", {})
            loan_info = record.get(f"Loan Score for {company_info.get('company_name')}", {})

            if company_info:
                company_name = company_info.get("company_name", "")
                company_number = company_info.get("company_number", "")
                uk_city_location = company_info.get("uk_city_location", "")
                registered_address = company_info.get("registered_address", {}) or {}
                address_line_1 = registered_address.get("address_line_1", "")
                address_line_2 = registered_address.get("address_line_2", "")
                locality = registered_address.get("locality", "")
                postal_code = registered_address.get("postal_code", "")
                region = registered_address.get("region", "")
                active_since = company_info.get("active_since_date")
                currently_active = company_info.get("currently_active", "")
                is_the_company_active = company_info.get("is_the_company_active", "")
                industry_info = company_info.get("industry_of_the_company_from_sic", []) or []
                sector = [_.get("Sector", "") for _ in industry_info if _]
                sub_sector = [_.get("Sub-sector", "") for _ in industry_info if _]
                sic_code = [_.get("Sic Code", "") for _ in industry_info if _]
                vat_registered = company_info.get("vat_registered", "")

            if filing_info:
                latest_account_filing_date = filing_info.get("latest_account_filing_date", "")
                account_filing_in_past_month = filing_info.get("account_filing_in_past_month", "")
                months_since_last_filing = filing_info.get("months_since_last_filing", "")
                secretary_or_agent_used_for_filing = filing_info.get("secretary_or_agent_used_for_filing", "")
                accounts_filed_early = filing_info.get("accounts_filed_early", "")
            
            if legal_info:
                outstanding_count = legal_info.get("outstanding_count", "")
                satisfied_count = legal_info.get("satisfied_count", "")
                has_debentures_or_charges = legal_info.get("has_debentures_or_charges", "")
                debentures_status = legal_info.get("debentures_status", "")
                has_ccjs = legal_info.get("has_ccjs", "")
                ccjs_status = legal_info.get("ccjs_status", "")
            else:
                outstanding_count = ""
                satisfied_count = ""
                has_debentures_or_charges = ""
                debentures_status = ""
                has_ccjs = ""
                ccjs_status = ""

            if loan_info:
                fds = loan_info.get("FDS", "")
                if fds and isinstance(fds, Dict):
                    fds_score = fds.get("score", "") or ""
                else:
                    fds_score = fds
                                
                lui = loan_info.get("LUI", "")
                if lui and isinstance(lui, Dict):
                    lui_score = lui.get("score", "") or ""
                else:
                    lui_score = lui
                
                loan_cap = loan_info.get("Loan Capacity", "")
                if loan_cap and isinstance(loan_cap, Dict):
                    if loan_cap.get("funding_range", None):
                        loan_cap_score = loan_cap["funding_range"]
                    elif loan_cap.get("range", None):
                        loan_cap_score = loan_cap["range"]
                    else:
                        loan_cap_score = ""

                else:
                    loan_cap_score = loan_cap
                                    
                market_signals = loan_info.get("Market Signals", "")
                if market_signals and isinstance(market_signals, Dict):
                    market_signal_score = market_signals.get("score", "") or ""
                else:
                    market_signal_score = market_signals
                
                new_broker = loan_info.get("Would work with a new loan broker", "")
                if new_broker and isinstance(new_broker, Dict):
                    new_broker_score = new_broker.get("score", "") or ""
                else:
                    new_broker_score = new_broker

                loan_need_today = loan_info.get("Needs a loan today score", "")
                if loan_need_today and isinstance(loan_need_today, Dict):
                    loan_need_today_score = loan_need_today.get("score", "") or ""
                else:
                    loan_need_today_score = loan_need_today

                timing = loan_info.get("Recommended Timing", "")
                top_3_risks = loan_info.get("Top 3 Risks for Lender", "")
                top_3_purposes = loan_info.get("Top 3 Loan Purposes", "")
            else:
                fds_score = ""
                lui_score = ""
                loan_cap_score = ""
                market_signal_score = ""
                new_broker_score = ""
                loan_need_today_score = ""
                timing = ""
                top_3_risks = ""
                top_3_purposes = ""

            if director_info:
                director_ages = director_info.get("director_age_years", [])
                other_directors = director_info.get("names_of_other_directors", [])
                age_lookup = {}
                for obj in director_ages:
                    age_lookup.update(obj)
                director_age = None
                for director in other_directors:
                    director_age = age_lookup.get(director, "")
                    director_ethnicity = record.get(f"Ethnicity of {director}")
                    full_name = director_ethnicity.get("full_name")
                    ethnicity = director_ethnicity.get("ethnicity")
                    skin_colour = director_ethnicity.get("skin_colour")
                    all_data.append({
                        "Name": full_name,
                        "Age": director_age,
                        "Data Source": source,
                        "All Companies": all_companies,
                        "Ethnicity": ethnicity,
                        "Skin Colour": skin_colour,
                        "Company Name": company_name,
                        "Company Number": company_number,
                        "Number of Directors": len(other_directors),
                        "Names of Other Directors": ", ".join(other_directors),
                        "Location": uk_city_location,
                        "Address 1": address_line_1,
                        "Address 2": address_line_2,
                        "Locality": locality,
                        "Postal Code": postal_code,
                        "Regon": region,
                        "Active Since": active_since,
                        "Currently Active": currently_active,
                        "Is company Active": is_the_company_active,
                        "Months of Trading": f"{months_active(active_since)}",
                        "Sector": sector,
                        "Sub Sector": sub_sector,
                        "SIC CODE": sic_code,
                        "VAT Reg": vat_registered,
                        "Latest Filing Date": latest_account_filing_date,
                        "Account Filing in Last Month": account_filing_in_past_month,
                        "Months Since Last Filing": months_since_last_filing,
                        "Secretary or Agent Used for Filing": secretary_or_agent_used_for_filing,
                        "Accounts Filed Early": accounts_filed_early,
                        "Outstanding Charges": outstanding_count,
                        "Satisfied Charges": satisfied_count,
                        "Has Charges": has_debentures_or_charges,
                        "Charges Status": debentures_status,
                        "Has CCJS": has_ccjs,
                        "CCJS Status": ccjs_status,
                        "FDS": fds_score or "",
                        "LUI": lui_score or "",
                        "Loan Capacity": loan_cap_score or "",
                        "Market Signals": market_signal_score or "",
                        "Would work with a new loan broker": new_broker_score or "",
                        "Needs a loan today score": loan_need_today_score or "",
                        "Recommended Timing": timing or "",
                        "Top 3 Risks for Lender": top_3_risks or "",
                        "Top 3 Loan Purposes": top_3_purposes or ""
                    })
    return all_data


def process_json_in_batches(input_path: str, output_csv: str, batch_size: int = 1000):
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    total = len(data)
    print(f"Total records: {total}")
    
    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1} ({i} to {min(i + batch_size, total) - 1})")

        all_rows = []
        for record in batch:
            all_rows.extend(process_per_director(record))

        df = pd.DataFrame(all_rows)
        df.to_csv(output_csv, mode='a', index=False, header=(i == 0))

    print(f"CSV successfully written to {output_csv}")



if __name__ == "__main__":
    process_json_in_batches("result.json", "new_directors_output.csv", batch_size=1000)
