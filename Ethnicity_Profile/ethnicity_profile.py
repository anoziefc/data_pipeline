from fake_useragent import UserAgent
from pydantic import BaseModel, Field
from typing import Dict, Optional, Any
import aiohttp
import asyncio
import json
import os
from aiolimiter import AsyncLimiter


class AnswerFormat(BaseModel):
    full_name: str = Field(..., alias="Full Name", description="The Full nNme of the person.")
    ethnicity: str = Field(..., alias="Ethnicity", description="The inferred ethnicity from the predefined list.")
    skin_colour: str = Field(..., alias="Skin Colour", description="The inferred skin color from the predefined list.")


class Prompt:
    __gpt_content = "You are an expert in sociolinguistics and demographic inference."

    def __init__(self, full_name, skin_colors=None, ethnicities=None):
        self.full_name = full_name
        self.skin_color = skin_colors if skin_colors is not None else [
            "Black", "Brown", "Varies", "White"
        ]
        self.ethnicity = ethnicities if ethnicities is not None else ["British Isles", "Central Europe", "Eastern Europe", "African", "Latin American", "Asian"]
    
    def construct_prompt(self) -> str:
        prompt_parts = [
            self.__gpt_content,
            f"\nObjective: Given the full name of a person and a predefined list of ethnic and skin colour categories, identify the most likely ethnicity and skin colour based on the name alone.",
            "Use linguistic, geographic, and cultural patterns in naming to guide your inference.",
            "If uncertain, choose the closest plausible match rather than defaulting to unknown.",
            f"Full Name: {self.full_name}",
            f"Available Ethnicity Options: {self.ethnicity}",
            f"Available Skin Colour Options: {self.skin_color}",
            "Output a JSON object with only three fields: 'Full Name', 'Ethnicity', and 'Skin Colour'. Ensure 'Ethnicity' and 'Skin Colour' values are *strictly* chosen from the provided lists only.",
        ]

        return "\n".join(prompt_parts)


class GeminiChat:
    __model_name: str = "gemini-2.0-flash"
    __base_url: str = "https://generativelanguage.googleapis.com/v1beta/models"

    def __init__(self, api_key: str, prompt: Prompt):
        self.prompt = prompt
        self.api_key = api_key
        self.url = f"{self.__base_url}/{self.__model_name}:generateContent"
        if not self.api_key:
            raise EnvironmentError("DANIEL_GEMINI_KEY environment variable not set")
        try:
            self.ua = UserAgent().random
        except Exception:
            self.ua = "Mozilla/5.0 (compatible; PerplexityBot/1.0)"

    async def send_request(self, session: aiohttp.ClientSession, timeout: float = 30.0) -> tuple[str, int]:
        full_prompt = self.prompt.construct_prompt()

        response_schema = {
            "type": "OBJECT",
            "properties": {
                "Full Name": {"type": "STRING"},
                "Ethnicity": {"type": "STRING"},
                "Skin Colour": {"type": "STRING"}
            },
            "required": ["Full Name", "Ethnicity", "Skin Colour"]
        }

        body = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": full_prompt
                        }
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.5,
                "responseMimeType": "application/json",
                "responseSchema": response_schema
            }
        }

        headers = {
            "x-goog-api-key": self.api_key,
            "Content-Type": "application/json",
            "User-Agent": self.ua
        }

        try:
            async with session.post(
                self.url,
                headers=headers,
                json=body,
                timeout=aiohttp.ClientTimeout(total=timeout)) as resp:

                response_data = await resp.json()

                if resp.status == 200:
                    try:
                        candidates = response_data.get("candidates", [])
                        if not candidates:
                            print("Gemini API returned 200 but no candidates.")
                            return None, 200
                        generated_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                        parsed_json = json.loads(generated_text)
                        return AnswerFormat(**parsed_json)
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse JSON response: {e}, Response: {generated_text}")
                        raise ValueError("Invalid JSON response from Gemini API") from e
                    except Exception as e:
                        print(f"Unexpected error during response parsing: {e}")
                        raise RuntimeError("Error processing Gemini API response") from e
                else:
                    print(f"Gemini API returned error {resp.status}")
                    resp.raise_for_status()
                    return None, resp.status
        except aiohttp.ClientError as e:
            print(f"HTTP Client Error during API call: {e}")
            raise ConnectionError(f"Network error during API call: {e}") from e
        except asyncio.TimeoutError:
            print("Gemini API request timed out.")
            raise TimeoutError("Gemini API request timed out")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise RuntimeError("Unexpected error during API call to Gemini") from e

async def run_ethnicity_check(logger, data: Dict[str, Any], limiter: Optional[AsyncLimiter] = None) -> Optional[Dict[str, Any]]:
    gemini_api_key = os.environ.get("DANIEL_GEMINI_KEY")
    if not gemini_api_key:
        logger.error("Error: DANIEL_GEMINI_KEY environment variable not set.")
        return
    async with aiohttp.ClientSession() as session:
        try:
            names = []
            matched_company_records = data.get("matched_company_records")
            if len(matched_company_records) >= 1:
                for k, v in data.items():
                    if k == "matched_company_records":
                        for j in range(len(v)):
                            names.extend(v[j]["director_info"]["names_of_other_directors"])
                list(set(names))
            else:
                names.append(data.get("full_name", ""))
            
            if len(names) >= 1:
                for name in names:
                    prompt = Prompt(name)
                    ethnicity_chat = GeminiChat(gemini_api_key, prompt)
                    logger.info(f"Processing: {name}")
                    if limiter:
                        async with limiter:
                            response = await ethnicity_chat.send_request(session)
                    else:
                        response = await ethnicity_chat.send_request(session)
                    if response:
                        title = f"Ethnicity of {name}"
                        data[title] = response.model_dump()
                    else:
                        logger.warning(f"No result for {name}")
        except Exception as e:
            logger.warning(f"Ethnicity check failed for data: {e}", exc_info=True)
            return None
    return data
