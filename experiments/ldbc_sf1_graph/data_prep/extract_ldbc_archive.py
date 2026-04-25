from __future__ import annotations

import argparse
import shutil
import tarfile
from pathlib import Path

from experiments.ldbc_sf1_graph import config


SUPPORTED_SUFFIXES = (".tar.zst", ".tar.gz", ".tar")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract the LDBC SF1 archive into a working directory")
    parser.add_argument("--archive-path", type=Path, default=config.LDBC_ARCHIVE_PATH)
    parser.add_argument("--output-dir", type=Path, default=config.LDBC_EXTRACT_DIR)
    parser.add_argument("--force", action="store_true", help="Remove the output directory before extraction")
    return parser.parse_args()


def extract_archive(archive_path: Path, output_dir: Path, force: bool = False) -> Path:
    """Extract a supported LDBC archive and return the output directory.

    TODO:
    - Add explicit `.zst` decompression fallback if the system tarfile build does not support it.
    - Detect the top-level extracted folder and return that path instead of the requested output dir.
    """
    if not archive_path.exists():
        raise FileNotFoundError(f"Archive not found: {archive_path}")

    if force and output_dir.exists():
        shutil.rmtree(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    if archive_path.name.endswith(".tar.zst"):
        raise NotImplementedError(
            "`.tar.zst` extraction is not implemented in this scaffold yet. "
            "Use the system `tar --zstd` path or add `zstandard` support next."
        )

    if not archive_path.name.endswith(SUPPORTED_SUFFIXES):
        raise ValueError(f"Unsupported archive format: {archive_path}")

    with tarfile.open(archive_path) as archive:
        archive.extractall(output_dir)
    return output_dir


def main() -> None:
    args = parse_args()
    extracted_path = extract_archive(args.archive_path, args.output_dir, force=args.force)
    print(f"Extracted LDBC archive to: {extracted_path}")


if __name__ == "__main__":
    main()
