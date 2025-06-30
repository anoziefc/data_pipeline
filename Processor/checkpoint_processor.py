from dataclasses import dataclass, field
from typing import Dict, Optional, Set
import time
import datetime
import json
import os


@dataclass
class ProcessingState:
    processed_files: Set[str] = field(default_factory=set)
    processed_items: Dict[str, Set[str]] = field(default_factory=dict)
    current_file: Optional[str] = None
    total_processed: int = 0
    total_items: int = 0
    started_at: float = field(default_factory=time.monotonic)

    def save_checkpoint(self, logger, CONFIG: Dict):
        CONFIG["CHECKPOINT_DIR"].mkdir(parents=True, exist_ok=True)
        tmp_file = CONFIG["CHECKPOINT_DIR"] / "processing_state.tmp"
        final_file = CONFIG["CHECKPOINT_DIR"] / "processing_state.json"
        data = {
            "processed_files": list(self.processed_files),
            "processed_items": {k: list(v) for k, v in self.processed_items.items()},
            "current_file": self.current_file,
            "total_processed": self.total_processed,
            "total_items": self.total_items,
            "timestamp": datetime.datetime.now().isoformat()
        }
        with open(tmp_file, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_file, final_file)
        logger.info(f"Checkpoint saved: {self.total_processed}/{self.total_items} items processed")

    @classmethod
    def load_checkpoint(cls, logger, CONFIG: Dict):
        file = CONFIG["CHECKPOINT_DIR"] / "processing_state.json"
        if not file.exists():
            logger.info("No checkpoint found, starting fresh")
            return cls()
        try:
            with open(file, "r") as f:
                data = json.load(f)
            state = cls()
            state.processed_files = set(data.get("processed_files", []))
            state.processed_items = {k: set(v) for k, v in data.get("processed_items", {}).items()}
            state.current_file = data.get("current_file")
            state.total_processed = data.get("total_processed", 0)
            state.total_items = data.get("total_items", 0)
            logger.info(f"Checkpoint loaded: {state.total_processed}/{state.total_items} items already processed")
            return state
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}", exc_info=True)
            return cls()
