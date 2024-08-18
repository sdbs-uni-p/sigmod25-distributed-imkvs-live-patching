import os
import os.path
import shutil
import subprocess
import sys
import tempfile
from argparse import ArgumentParser, Namespace
from typing import List

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

DOWNLOAD_DUCKDB_VERSION_SCRIPT = os.path.join(SCRIPT_DIR, "download-duckdb-version")


def download_duckdb_version(version: str) -> str:
    print(f"Downloading DuckDB v{version}..")
    subprocess.run([DOWNLOAD_DUCKDB_VERSION_SCRIPT, version], check=True)
    return os.path.join(SCRIPT_DIR, f"duckdb-{version}")


def merge(
    source_version: str,
    source_files: List[str],
    destination_version: str,
    destination_file: str,
) -> None:
    source_duckdb_file = download_duckdb_version(source_version)
    destination_duckdb_file = download_duckdb_version(destination_version)

    if not os.path.exists(destination_file) and source_version == destination_version:
        # Special case: If source and target version are the same AND the file to merge into does not exist yet,
        # use the first (biggest) source file as a basis.
        print(f"Using {source_files[0]} as a basis for the output.")
        shutil.copyfile(source_files[0], destination_file)
        source_files = source_files[1:]

    def merge_src_into_dst(source_file: str):
        print("###" * 10)
        print(f"Merging {source_file}")
        with tempfile.TemporaryDirectory() as tmp_dir:
            export_cmd = [
                source_duckdb_file,
                source_file,
                "-readonly",
                "-c",
                f"EXPORT DATABASE '{tmp_dir}'",
            ]
            print(export_cmd)
            subprocess.run(export_cmd, check=True)

            def load(check_for_error=True):
                load_cmd = [
                    destination_duckdb_file,
                    destination_file,
                    "-no-stdin",
                    "-init",
                    os.path.join(tmp_dir, "load.sql"),
                ]
                print(load_cmd)
                proc = subprocess.run(
                    load_cmd,
                    check=True,
                    stdout=subprocess.PIPE if check_for_error else None,
                    stderr=subprocess.PIPE if check_for_error else None,
                )

                if check_for_error:
                    error = proc.stderr.decode("UTF-8")
                    if "Error:" in error:
                        print("Error while loading data..")
                        raise subprocess.CalledProcessError(
                            1, " ".join(load_cmd), stderr=error
                        )

            try:
                # Try to load data
                load()
            except subprocess.CalledProcessError:
                print(
                    "Loading data failed. Maybe schema was not created. Creating schema..."
                )
                # Schema may not exists, try to create schema first and load again
                schema_cmd = [
                    destination_duckdb_file,
                    destination_file,
                    "-no-stdin",
                    "-init",
                    os.path.join(tmp_dir, "schema.sql"),
                ]
                print(schema_cmd)
                subprocess.run(schema_cmd, check=True)

                load()

    for src_file in source_files:
        merge_src_into_dst(src_file)


def parse_args(input_args: List[str]) -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        "--source-version",
        default="0.9.2",
        type=str,
        help="The DuckDB version of the source database files.",
    )
    parser.add_argument(
        "--databases",
        nargs="+",
        required=True,
        help="A list of source DuckDB files. --source-version defines the version of these files.",
    )
    parser.add_argument(
        "--destination-version",
        default="0.9.2",
        type=str,
        help="The DuckDB version of the merged database files (output).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="all.duckdb",
        help="The output file in which all DuckDB files are merged into. "
        "--destination-version defines the version of this file.",
    )
    return parser.parse_args(input_args)


def main(input_args: List[str]) -> None:
    args = parse_args(input_args)
    if os.path.exists(args.output):
        print(f"Output {args.output} already exists!")
        exit(1)

    output_file = (
        args.output
        if os.path.isabs(args.output)
        else os.path.join(SCRIPT_DIR, args.output)
    )
    database_files = [
        file if os.path.isabs(file) else os.path.join(SCRIPT_DIR, file)
        for file in args.databases
    ]
    # Sort files. Use smallest file first
    database_files = sorted(database_files, key=os.path.getsize, reverse=True)
    print(f"Using database files: {database_files}")

    merge(args.source_version, database_files, args.destination_version, output_file)


if __name__ == "__main__":
    main(sys.argv[1:])
