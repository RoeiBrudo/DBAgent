
import sys

from evaluation.functions import run_experiment


def main() -> int:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "evaluation/config.yaml"
    out_dir = run_experiment(config_path)
    print(f"Wrote results to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())








