import json

from datasets import load_dataset
from huggingface_hub import snapshot_download

from data.helpers.utils import SPIDER_LOCAL_DIR, SPIDER_HF, CACHE_DIR, SPIDER_REPO, BIRD_HF, COSQL_LOCAL_DIR, SPARC_HF, BIRD_DB_ROOT


def spider_dataset_load():
    SPIDER_LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    spider = load_dataset(SPIDER_HF, cache_dir=str(CACHE_DIR))

    database_dir = SPIDER_LOCAL_DIR / "database"
    tables_json = SPIDER_LOCAL_DIR / "tables.json"
    if not (database_dir.exists() and tables_json.exists()):
        snapshot_download(
            repo_id=SPIDER_REPO,
            repo_type="dataset",
            cache_dir=str(CACHE_DIR),
            local_dir=str(SPIDER_LOCAL_DIR),
            allow_patterns=[
                "database/**",
                "tables.json",
                "**/*.sqlite",
                "**/*.sqlite3",
                "**/*.db",
            ],
        )
    return spider


def bird_dataset_load():
    bird = load_dataset(BIRD_HF, cache_dir=str(CACHE_DIR))

    bird_local_dir = BIRD_DB_ROOT.parent.parent
    bird_local_dir.mkdir(parents=True, exist_ok=True)
    if not BIRD_DB_ROOT.exists() or len(list(BIRD_DB_ROOT.rglob("*.sqlite"))) == 0:
        snapshot_download(
            repo_id=BIRD_HF,
            repo_type="dataset",
            cache_dir=str(CACHE_DIR),
            local_dir=str(bird_local_dir),
        )
    return bird


def cosql_dataset_load():
    sql_state_dir = COSQL_LOCAL_DIR / "sql_state_tracking"
    
    cosql_data = {}
    for split in ["train", "dev"]:
        candidates = [
            sql_state_dir / f"cosql_{split}.json",
            sql_state_dir / f"{split}.json",
        ]
        json_path = next((p for p in candidates if p.exists()), None)
        if json_path is not None:
            with open(json_path, 'r') as f:
                cosql_data[split] = json.load(f)
    return cosql_data


def sparc_dataset_load():
    sparc = load_dataset(SPARC_HF, cache_dir=str(CACHE_DIR))
    return sparc
