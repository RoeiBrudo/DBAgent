from itertools import chain

from data.helpers.collect import spider_dataset_load, bird_dataset_load, cosql_dataset_load, sparc_dataset_load
from data.helpers.normalize import spider_dataset_normalize, bird_dataset_normalize, cosql_dataset_normalize, \
    sparc_dataset_normalize
from data.helpers.test import spider_dataset_test, bird_dataset_test, cosql_dataset_test, sparc_dataset_test
from data.test import test_unified_dataset
from data.helpers.utils import add_database_paths, NORMALIZED_DIR, save_to_sqlite


def collect_normalize_data():
    print("=" * 80)
    print("PHASE 1: Individual Datasets")
    print("=" * 80)

    print("\n[Spider Dataset]")
    spider = spider_dataset_load()
    spider_dataset_test(spider)
    spider_normalized = spider_dataset_normalize(spider)

    print("\n[BIRD Dataset]")
    bird = bird_dataset_load()
    bird_dataset_test(bird)
    bird_normalized = bird_dataset_normalize(bird)

    print("\n[CoSQL Dataset]")
    cosql = cosql_dataset_load()
    cosql_dataset_test(cosql)
    cosql_normalized = cosql_dataset_normalize(cosql)

    print("\n[SParC Dataset]")
    sparc = sparc_dataset_load()
    sparc_dataset_test(sparc)
    sparc_normalized = sparc_dataset_normalize(sparc)

    print("=" * 80)
    print("PHASE 2: Unified Dataset")
    print("=" * 80)

    all_turns = list(chain(spider_normalized, bird_normalized, cosql_normalized, sparc_normalized))
    print(f"Total normalized turns: {len(all_turns)}")

    print("Adding database file paths...")
    all_turns = add_database_paths(all_turns)

    test_unified_dataset(all_turns)

    output_db = NORMALIZED_DIR / "turns.db"
    save_to_sqlite(all_turns, output_db)

    print("\n" + "=" * 80)
    print(f"✓ Pipeline Complete: {len(all_turns)} turns saved to {output_db}")
    print("=" * 80)

    return all_turns



if __name__ == "__main__":
    collect_normalize_data()