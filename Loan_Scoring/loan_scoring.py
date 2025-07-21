from fake_useragent import UserAgent
import aiohttp
import asyncio
import os
import json
import re
from Models.models import *
from typing import Dict, Optional, Any
from aiolimiter import AsyncLimiter


class Prompt:
    def __init__(self, business_details = None):
        self.business_details = business_details
    
    def construct_prompt(self) -> str:
        return f"""
Objective: Assess the loan needed for a UK business as of 19 June 2025, using data from the past 4 months as an expert loan broker looking for granular company specific information more than trends.

Output:

Evaluation Criteria:
 - FDS (0-100): Score based on signs of financial distress - cash flow issues, payment delays, covenant breaches, or asset sales. Include a 150-character summary with evidence.
 - LUI (0-100): Score for urgency of liquidity needs - tight deadlines, emergency funding requirements, or payroll strain. Include a 150-character summary with urgency/timeline.
 - Loan Capacity (0-100): Estimate funding amount (£ range), repayment capability, and top 3 ranked loan purposes.
 - Market Signals (0-100): Score based on external indicators - staff changes, legal disputes, or signs of business expansion (new offerings, partnerships, or acquisitions).
 - Would work with a new loan broker (0-100):
        - 0-50 = Unlikely
        - 51-70 = Possible
        - 71-90 = Likely
        - 91-100 = Only possible options
 - Needs a loan today score (0-100):
        - 0-50 = Unjustified loan need
        - 51-70 = Some specific loan need  
        - 71-90 = Strong specific loan need
        - 91-100 = Critical specific loan need
 - Recommended Timing: 
        - 1-3 months
        - 3-6 months
        - 6-12 months
        - 12-18 months
  - Top 3 Risks for Lender: Three major concerns for getting lender approval
  - Top 3 Loan Purposes: Three main business context specific uses cases for the loan
  - Sources: Cite URLs for all justifications

  INSTRUCTION:
    - Only return current data from Feb to June 2025.
    - Emphasize financial specifics, not general sector trends.
    - Source URLs must validate key points.
    - No assumptions without citation.
    - STRICTLY return only a JSON object with the evaluation criteria. Do not include code blocks or additional explanation.
"""

 
class PerplexityChat:
    def __init__(self, api_key: str, prompt: Prompt):
        self.prompt = prompt.construct_prompt()
        self.api_key = api_key
        if not self.api_key:
            raise EnvironmentError("PERPLEXITY_API_KEY environment variable not set")
        try:
            self.ua = UserAgent().random
        except Exception:
            self.ua = "Mozilla/5.0 (compatible; PerplexityBot/1.0)"

    async def send_request(self, session: aiohttp.ClientSession, timeout: float = 100.0) -> tuple[str, int]:
        if not self.prompt:
            return "Invalid prompt", 500

        body = {
            "model": "sonar-reasoning",
            "messages": [
                {"role": "user", "content": self.prompt}
            ],
            "temperature": 0.01
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": self.ua
        }

        try:
            async with session.post("https://api.perplexity.ai/chat/completions", headers=headers, json=body, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status == 200:
                    try:
                        result = await resp.json()
                        content = result.get("choices", [{}])[0].get("message", {}).get("content", {})
                        return content, 200
                    except Exception as parse_err:
                        return "Malformed response", 502
                else:
                    error_text = await resp.text()
                    return f"Error {resp.status}: {error_text}", resp.status

        except aiohttp.ClientError as e:
            return f"HTTP Client Error: {str(e)}", 503
        except asyncio.TimeoutError:
            return "Request timed out", 504
        except Exception as e:
            return f"Unexpected Error: {str(e)}", 500


def extract_json_from_markdown_reasoning(response: str) -> Dict[str, Any]:
    marker = "</think>"
    idx = response.rfind(marker)
    
    if idx == -1:
        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            raise ValueError("No </think> marker found and content is not valid JSON") from e

    json_str = response[idx + len(marker):].strip()

    if json_str.startswith("```json"):
        json_str = json_str[len("```json"):].strip()
    if json_str.startswith("```"):
        json_str = json_str[3:].strip()
    if json_str.endswith("```"):
        json_str = json_str[:-3].strip()
    
    try:
        parsed_json = json.loads(json_str)
        return parsed_json
    except json.JSONDecodeError as e:
        raise ValueError("Failed to parse valid JSON from response content") from e

def extract_json_from_markdown(text):
    match = re.search(r'```json\s*(\{.*?\})\s*```', text, re.DOTALL)
    if match:
        json_str = match.group(1)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            print("JSON decoding failed:", e)
            return None
    else:
        print("No JSON found in the string.")
        return None

def extract_and_inject_json(completion: Dict[str, Any]) -> Dict[str, Any]:
    try:
        raw_content = completion.strip()

        if raw_content.startswith("```"):
            lines = raw_content.splitlines()
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            raw_content = "\n".join(lines).strip()

        start = raw_content.find("{")
        end = raw_content.rfind("}")
        json_candidate = None
        if start != -1 and end != -1 and start < end:
            json_candidate = raw_content[start:end+1].strip()
        
        if json_candidate:
            try:
                parsed_json = json.loads(json_candidate)
            except json.JSONDecodeError:
                match = re.search(r"({.*})", raw_content, re.DOTALL)
                if match:
                    json_candidate = match.group(1).strip()
                    parsed_json = json.loads(json_candidate)
                else:
                    raise ValueError("No valid JSON object found via regex.")
        else:
            match = re.search(r"({.*})", raw_content, re.DOTALL)
            if match:
                json_candidate = match.group(1).strip()
                parsed_json = json.loads(json_candidate)
            else:
                raise ValueError("No JSON object found in content.")
        return parsed_json
    except Exception as e:
        raise ValueError(f"Error extracting valid JSON from content: {e}")


async def run_loan_scoring(logger, data: Dict[str, Any], limiter: Optional[AsyncLimiter] = None):
    perplexity_api_key = os.environ.get("PERPLEXITY_API_KEY")
    if not perplexity_api_key:
        logger.error("Error: PERPLEXITY_API_KEY environment variable not set.")
        return

    matched_company_records = data.get("matched_company_records")
    all_companies = data.get("all_companies")
    if len(matched_company_records) >= 1:
        for company in matched_company_records:
                prompt_obj = Prompt(business_details=company["company_info"])
                perplexity_chat = PerplexityChat(api_key=perplexity_api_key, prompt=prompt_obj)
                async with aiohttp.ClientSession() as session:
                    if limiter:
                        async with limiter:
                            content, status = await perplexity_chat.send_request(session)
                    else:
                            content, status = await perplexity_chat.send_request(session)

                    title = f"Loan Score for {company["company_info"]["company_name"]}"
                    if content and content.strip().startswith("{"):
                        try:
                            data[title] = json.loads(content)
                        except json.JSONDecodeError as e:
                            logger.error("JSON decoding failed:", e)
                            data[title] = {}
                    else:
                        try:
                            data[title] = extract_json_from_markdown(content)
                        except Exception as e:
                            logger.error("Received empty or invalid response:", repr(content))
                            data[title] = {}
    else:
        for company in all_companies:
                prompt_obj = Prompt(business_details=company)
                perplexity_chat = PerplexityChat(api_key=perplexity_api_key, prompt=prompt_obj)
                async with aiohttp.ClientSession() as session:
                    if limiter:
                        async with limiter:
                            content, status = await perplexity_chat.send_request(session)
                    else:
                            content, status = await perplexity_chat.send_request(session)

                    title = f"Loan Score for {company}"
                    if content and content.strip().startswith("{"):
                            try:
                                data[title] = json.loads(content)
                            except json.JSONDecodeError as e:
                                logger.error("JSON decoding failed:", e)
                                data[title] = {}
                    else:
                        try:
                            data[title] = extract_json_from_markdown(content)
                        except Exception as e:
                            logger.error("Received empty or invalid response:", repr(content))
                            data[title] = {}
    return data
