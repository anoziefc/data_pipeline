import asyncio
import json
import logging
from pathlib import Path
from Processor.data_pipeline import DataPipeline
from Processor.checkpoint_processor import ProcessingState
from Processor.company_matcher import match_companies
from Company_House.company_house import run_business_profiling
from Ethnicity_Profile.ethnicity_profile import run_ethnicity_check
from Loan_Scoring.loan_scoring import run_loan_scoring
from typing import List
from aiolimiter import AsyncLimiter


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("perplexity_processing.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

CONFIG = {
    "TRUSTPILOT_DATA_PATH": Path("data/trust_pilot/trust_pilot.json"),
    "TAX_DEFAULTERS_DATA_PATH": Path("data/tax_defaulters/tax_defaulters.json"),
    "BIDSTATS_DATA_PATH": Path("data/bidstats/bidstats.json"),
    "MATCHED": Path("data/matched/matched.json"),
    "SOURCE_DATA_PATH": Path("data/data.json"),
    "RESPONSE_DATA_PATH": Path("data/result.json"),
    "ENRICHED_DATA_PATH": Path("data/enriched/enriched.json"),
    "CHECKPOINT_DIR": Path("checkpoints/"),
    "CHECKPOINT_INTERVAL": 50,
    "QUEUE_SIZE": 100,
    "MAX_CONCURRENT_REQUESTS": 50
}

def prepare_file(file_path: Path, result_data: List):
    with open(file_path, "r") as f:
        file_content = json.loads(f.read())

    if "trust_pilot" in file_path:
        for content in file_content:
            data = {
                "source": "Trust Pilot",
                "full_name": content["full_name"],
                "companies": content["companies"]
            }
            result_data.append(data)
    elif "bidstats" in file_path:
        for content in file_content:
            suppliers = [_["name"] for _ in content["suppliers"] if len(_) >= 1]
            if len(suppliers) >= 1:
                data = {
                    "source": "BidStats",
                    "companies": suppliers
                }
                result_data.append(data)
    elif "tax_defaulters" in file_path:
        for content in file_content:
            val = []
            val.append(content["Name"])
            data = {
                "source": "Tax Default",
                "companies": val
            }
            result_data.append(data)

async def runner(path, file_name, log_file, config, task_to_run, rate_limit, max_concurrent_sessions):
    ps = ProcessingState()
    pipeline = DataPipeline(ps, log_file, dataset_paths=[path], CONFIG=config, resume=True)

    limiter = AsyncLimiter(*rate_limit) if rate_limit else None
    semaphore = asyncio.Semaphore(max_concurrent_sessions) if max_concurrent_sessions else None

    producer_tasks = [
        asyncio.create_task(pipeline.producer(file_name, path))
    ]

    consumer_tasks = [
        asyncio.create_task(pipeline.consumer(task_to_run, i, limiter, semaphore))
            for i in range(CONFIG["MAX_CONCURRENT_REQUESTS"])
    ]

    await asyncio.gather(*producer_tasks)

    for _ in range(CONFIG["MAX_CONCURRENT_REQUESTS"]):
        await pipeline.queue.put(None)

    await asyncio.gather(*consumer_tasks)
    return pipeline

async def stage_one(path, file_name, log_file, config, run_process, match_data):
    runner_instance = await runner(path, file_name, log_file, config, run_process, rate_limit=(2, 1), max_concurrent_sessions=1)
    runner_instance.state.save_checkpoint(log_file, config)

    ret = Path("data/matched/matched.json")
    matched = match_companies(runner_instance.results, match_data)
    with open(ret, "w") as f:
        json.dump(matched, f, indent=4)
    log_file.info("Stage One complete")
    return matched

async def stage_two(path, file_name, log_file, config, run_process):
    runner_instance = await runner(path, file_name, log_file, config, run_process, rate_limit=(20, 1), max_concurrent_sessions=50)
    runner_instance.state.save_checkpoint(log_file, config)
    try:
        ret = Path("data/enriched/enriched.json")
        with open(ret, "w", encoding="utf-8") as f:
            json.dump(runner_instance.results, f, indent=4, ensure_ascii=False)
        log_file.info("Stage Two Completed")
        return ret
    except Exception as e:
        log_file.error(f"Failed to save results: {e}", exc_info=True)

async def stage_three(path, file_name, log_file, config, run_process):
    runner_instance = await runner(path, file_name, log_file, config, run_process, rate_limit=(6, 1), max_concurrent_sessions=20)
    runner_instance.state.save_checkpoint(log_file, config)
    try:
        with open(config["RESPONSE_DATA_PATH"], "w", encoding="utf-8") as f:
            json.dump(runner_instance.results, f, indent=4, ensure_ascii=False)
        log_file.info(f"Results saved to {config['RESPONSE_DATA_PATH']}")
        return config["RESPONSE_DATA_PATH"]
    except Exception as e:
        log_file.error(f"Failed to save results: {e}", exc_info=True)
    runner_instance.state.save_checkpoint(log_file, config)

async def main():
    dataset_paths = [
        ("data", CONFIG["SOURCE_DATA_PATH"].parent),
        ("trust_pilot", CONFIG["TRUSTPILOT_DATA_PATH"].parent),
        ("bidstats", CONFIG["BIDSTATS_DATA_PATH"].parent),
        ("tax_defaulters", CONFIG["TAX_DEFAULTERS_DATA_PATH"].parent),
        ("matched", CONFIG["MATCHED"].parent),
        ("enriched", CONFIG["ENRICHED_DATA_PATH"].parent)
    ]

    first_stage_list = ["trust_pilot", "bidstats", "tax_defaulters"]
    dt = []

    for n in range(len(dataset_paths)):
        pth = f"{dataset_paths[n][1]}/{dataset_paths[n][0]}.json"
        if dataset_paths[n][0] in first_stage_list:
            prepare_file(pth, dt)

    stage_one_file_name = dataset_paths[0][0]
    stage_one_path = dataset_paths[0][1]
    stage_one_file = CONFIG["SOURCE_DATA_PATH"]

    stage_two_file_name = dataset_paths[4][0]
    stage_two_path = dataset_paths[4][1]

    with open(stage_one_file, "w") as ff:
        json.dump(dt, ff, indent=4)
    
    await stage_one(stage_one_path, stage_one_file_name, logger, CONFIG, run_business_profiling, dt)
    await stage_two(stage_two_path, stage_two_file_name, logger, CONFIG, run_ethnicity_check)
    stage_three_file_name = dataset_paths[5][0]
    stage_three_path = dataset_paths[5][1]
    await stage_three(stage_three_path, stage_three_file_name, logger, CONFIG, run_loan_scoring)

    return


if __name__ == "__main__":
    asyncio.run(main())