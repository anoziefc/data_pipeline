import asyncio
import json
import os
from pathlib import Path
from typing import Dict, Optional, Any, List
from Company_House.company_house import run_business_profiling
from Ethnicity_Profile.ethnicity_profile import run_ethnicity_check
from dataclasses import asdict


class DataPipeline:
    def __init__(self, ProcessingState, logger, dataset_paths: List[Path], CONFIG: Dict, resume: bool = True):
        self.logger = logger
        self.CONFIG = CONFIG
        self.queue = asyncio.Queue(maxsize=self.CONFIG["QUEUE_SIZE"])
        self.dataset_paths = dataset_paths
        self.state = ProcessingState.load_checkpoint(self.logger, self.CONFIG) if resume else ProcessingState()
        self.processing_complete = asyncio.Event()
        self.results = []
    
    async def scan_files(self, file_location: Path) -> List[str]:
        files = [
            f for f in os.listdir(file_location)
            if f.endswith(".json") and f not in self.state.processed_files
        ]
        if self.state.current_file and self.state.current_file in files:
            files.remove(self.state.current_file)
            files.insert(0, self.state.current_file)
        return files

    async def producer(self, dataset_label: str, file_path: Path):
        try:
            files = await self.scan_files(file_path)
            if not files:
                self.logger.warning("No new files to process")
                self.processing_complete.set()
                return

            self.logger.info(f"Found {len(files)} files to process")

            for f in files:
                self.state.current_file = f
                path = file_path / f
                try:
                    data = await asyncio.to_thread(lambda: json.load(path.open("r", encoding="utf-8")))
                    if not isinstance(data, dict) and not isinstance(data, list):
                        self.logger.warning(f"{dataset_label}: Skipping {f} - invalid format")
                        continue

                    key = f"{dataset_label}:{f}"
                    self.state.processed_items.setdefault(key, set())

                    if isinstance(data, dict):
                        for item_id, item_data in data.items():
                            if item_id not in self.state.processed_items[key]:
                                await self.queue.put({
                                    "dataset": dataset_label,
                                    "file": f,
                                    "id": item_id,
                                    "data": item_data
                                })
                            else:
                                continue
                    else:
                        for id in range(len(data)):
                            if id not in self.state.processed_items[key]:
                                await self.queue.put({
                                    "dataset": dataset_label,
                                    "file": f,
                                    "id": id,
                                    "data": data[id]
                                })
                    
                    self.state.processed_files.add(f)
                    self.logger.info(f"File {f} is completely processed")

                except Exception as e:
                    self.logger.error(f"Failed reading {f}: {e}", exc_info=True)

        except Exception as e:
            self.logger.error(f"Producer error: {e}", exc_info=True)

    async def consumer(self, process, worker_id: int):
        try:
            while True:
                item = await self.queue.get()
                if item is None:
                    self.logger.info(f"Worker-{worker_id} received shutdown signal")
                    break

                try:
                    dataset = item["dataset"]
                    _file = item["file"]
                    item_id = item["id"]
                    data = item["data"]
                    result = await self.process_item(process, dataset, _file, item_id, data)
                    if result:
                        key = f"{dataset}:{_file}"
                        self.state.processed_items.setdefault(key, set()).add(item_id)
                        self.state.total_processed += 1
                        self.results.append(result)
                    
                    if self.state.total_processed % self.CONFIG["CHECKPOINT_INTERVAL"] == 0:
                        self.state.save_checkpoint(self.logger, self.CONFIG)
                except Exception as e:
                    self.logger.error(f"Consumer error on item {item.get('id')}: {e}", exc_info=True)
                finally:
                    self.queue.task_done()
        except Exception as e:
            self.logger.error(f"Consumer error on item {item.get('id')}: {e}", exc_info=True)

    async def process_item(self, process, dataset: str, f: str, item_id: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.logger.debug(f"[{dataset}] Processed item {item_id} from {f}")
        retVal = await process(self.logger, data)
        return retVal
