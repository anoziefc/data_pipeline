from dataclasses import dataclass
from pydantic import BaseModel, Field, HttpUrl, conlist
from typing import Optional, Literal, List, Annotated 
from annotated_types import Len 


@dataclass
class CompanyInfo:
    company_name: Optional[str] = None
    company_number: Optional[str] = None
    uk_city_location: Optional[str] = None
    registered_address: Optional[str] = None
    active_since_date: Optional[str] = None
    currently_active: Optional[Literal["Yes", "No"]] = "No"
    is_the_company_active: Optional[Literal["Yes", "No"]] = "No"
    industry_of_the_company_from_sic: Optional[str] = None
    vat_registered: Optional[Literal["Yes", "No"]] = "No"


@dataclass
class DirectorInfo:
    number_of_directors: Optional[int] = None
    names_of_other_directors: Optional[str] = None
    linkedin_profile_urls_of_directors: Optional[str] = None
    tenure_of_directors: Optional[int] = None
    director_age_years: Optional[int] = None
    change_of_director_in_past_3_months: Optional[Literal["Yes", "No"]] = "No"
    date_of_last_change_of_directors: Optional[str] = None


@dataclass
class FilingInfo:
    latest_account_filing_date: Optional[str] = None
    account_filing_in_past_month: Optional[Literal["Yes", "No", "Unavailable"]] = "Unavailable"
    months_since_last_filing: Optional[int] = None
    secretary_or_agent_used_for_filing: Optional[Literal["Yes", "No", "Unavailable"]] = "Unavailable"
    accounts_filed_early: Optional[Literal["Yes", "No", "Unavailable"]] = "Unavailable"


@dataclass
class FinancialInfo:
    revenue_estimate: Optional[float] = None
    months_of_trading: Optional[int] = None
    fixed_assets_value: Optional[float] = None
    total_assets_endole: Optional[float] = None
    total_liabilities_endole: Optional[float] = None
    net_assets_endole: Optional[float] = None
    debt_ratio_endole: Optional[float] = None
    staff_count: Optional[int] = None


@dataclass
class LegalInfo:
    outstanding_count: Optional[str]
    satisfied_count: Optional[str]
    has_debentures_or_charges: Optional[Literal["Yes", "No", "Unavailable"]] = "Unavailable"
    debentures_status: Optional[Literal["Has Outstanding", "All Satisfied", "Unavailable"]] = "Unavailable"
    has_ccjs: Optional[Literal["Yes", "No", "Unavailable"]] = "Unavailable"
    ccjs_status: Optional[Literal["Outstanding", "Satisfied", "Unavailable"]] = "Unavailable"


@dataclass
class DirectorLoanInfo:
    director_loan_using_company_filings_pdf: Optional[Literal["Yes", "No", "Unavailable"]] = "Unavailable"
    director_loan_amount: Optional[float] = None
    director_loan_date: Optional[str] = None


@dataclass
class BusinessProfile:
    company_info: CompanyInfo
    director_info: DirectorInfo
    filing_info: FilingInfo
    legal_info: LegalInfo


@dataclass
class CompanyInput(BaseModel):
    person_name: str
    company_name: str
    town: str
    county: str
    date: str


@dataclass
class LegalCase(BaseModel):
    date: str
    outcome: Literal["Positive for Company", "Negative for Company", "Ongoing"]
    cost: float


@dataclass
class ScoreSummary(BaseModel):
    score: int
    summary: str


@dataclass
class CreditHealthOutput(BaseModel):
    company_name: str
    company_number: Optional[str]

    distress_indicator: ScoreSummary
    growth_signals: ScoreSummary

    loan_urgency: dict
    loan_need: dict
    loan_purposes: List[str]

    staff_growth: int
    legal_cases: List[LegalCase]


class EvaluationResponse(BaseModel):
    """
    Pydantic model for the comprehensive evaluation response.
    """
    fds_score: int = Field(
        ...,
        ge=0,
        le=100,
        description="FDS (Financial Distress Score): Score based on signs of financial distress."
    )
    fds_summary: str = Field(
        ...,
        max_length=150,
        description="150-character summary with evidence for FDS."
    )
    lui_score: int = Field(
        ...,
        ge=0,
        le=100,
        description="LUI (Liquidity Urgency Index): Score for urgency of liquidity needs."
    )
    lui_summary: str = Field(
        ...,
        max_length=150,
        description="150-character summary with urgency/timeline for LUI."
    )
    loan_capacity_score: int = Field(
        ...,
        ge=0,
        le=100,
        description="Loan Capacity: Estimated funding amount, repayment capability, and top 3 ranked loan purposes score."
    )
    market_signals_score: int = Field(
        ...,
        ge=0,
        le=100,
        description="Market Signals: Score based on external indicators like staff changes, legal disputes, or business expansion."
    )
    would_work_with_new_loan_broker_score: int = Field(
        ...,
        ge=0,
        le=100,
        description="Score indicating willingness to work with a new loan broker (0-50=Unlikely, 51-70=Possible, 71-90=Likely, 91-100=Only possible options)."
    )
    needs_a_loan_today_score: int = Field(
        ...,
        ge=0,
        le=100,
        description="Score indicating the urgency of needing a loan today (0-50=Unjustified, 51-70=Some specific, 71-90=Strong specific, 91-100=Critical specific)."
    )
    recommended_timing: Literal["1-3 months", "3-6 months", "6-12 months", "12-18 months"] = Field(
        ...,
        description="Recommended timing for the loan."
    )
    top_3_risks_for_lender: Annotated[
        List[str],
        Len(min_length=3, max_length=3),
        Field(description="Three major concerns for getting lender approval.")
    ]
    top_3_loan_purposes: Annotated[
        List[str],
        Len(min_length=3, max_length=3),
        Field(description="Three main business context-specific use cases for the loan.")
    ]
    sources: List[HttpUrl] = Field(
        ...,
        description="List of URLs for all justifications and evidence."
    )