import argparse

from experiments.cross_db_graph.runner import main as runner_main


def main():
    parser = argparse.ArgumentParser(description="Deprecated wrapper for cold-ish graph benchmarks")
    parser.add_argument("--engine", choices=["postgres", "arangodb"], required=True)
    parser.parse_args()
    raise SystemExit(
        "Use `python -m experiments.cross_db_graph.runner --engine <engine> --mode coldish` instead."
    )


if __name__ == "__main__":
    main()
