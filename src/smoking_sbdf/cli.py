"""Command-line interface for smoking-sbdf."""

from __future__ import annotations

import argparse
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import sys
from typing import Sequence

from . import SBDFError, convert


PROJECT_README = """# SBDF 변환 안내

이 프로젝트는 `smoking-sbdf`로 CSV, Parquet 또는 Python DataFrame을
Spotfire SBDF 1.0 파일로 변환한다.

## CLI

```bash
smoking-sbdf convert input.csv output.sbdf
smoking-sbdf convert input.parquet output.sbdf
smoking-sbdf convert input.csv output.sbdf --workers 4
smoking-sbdf convert input.parquet output.sbdf --workers 3
smoking-sbdf -help
```

입력 형식을 자동 판별할 수 없으면 `--input-format`을 지정한다.

```bash
smoking-sbdf convert input.data output.sbdf --input-format csv
```

## Python

```python
from smoking_sbdf import convert

convert(
    "input.parquet",
    "output.sbdf",
    workers=3,
    adaptive_workers=True,
)
```

DataFrame이 메모리에 있으면 다음처럼 저장한다.

```python
import smoking_sbdf

dataframe.to_sbdf("output.sbdf")
```

## LLM 작업 지침

- 입력 파일을 변경하거나 삭제하지 않는다.
- 출력 경로는 입력 경로와 다르게 지정한다.
- CSV, Parquet, Parquet dataset과 manifest만 CLI 입력으로 사용한다.
- `--workers`는 1–8이며 CSV 기본값은 1, Parquet 기본 최대값은 3이다.
- Parquet worker 수를 강제하려면 `--no-adaptive-workers`를 함께 사용한다.
- row-key Parquet sidecar가 필요하면 Python API의 `row_key_columns`를 사용한다.
- 정확한 옵션은 `smoking-sbdf -help`와 `smoking-sbdf convert -help`에서 확인한다.
"""


def _package_version() -> str:
    try:
        return version("smoking-sbdf")
    except PackageNotFoundError:
        return "0+unknown"


def _column_types(values: list[str]) -> dict[str, str] | None:
    resolved: dict[str, str] = {}
    for value in values:
        name, separator, sbdf_type = value.partition("=")
        if not separator or not name.strip() or not sbdf_type.strip():
            raise ValueError(
                f"invalid --column-type {value!r}; expected COLUMN=SBDF_TYPE"
            )
        name = name.strip()
        if name in resolved:
            raise ValueError(f"duplicate --column-type for {name!r}")
        resolved[name] = sbdf_type.strip()
    return resolved or None


def _worker_count(value: str) -> int:
    try:
        workers = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "workers must be an integer from 1 to 8"
        ) from error
    if not 1 <= workers <= 8:
        raise argparse.ArgumentTypeError("workers must be between 1 and 8")
    return workers


def _add_help_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-h",
        "--help",
        "-help",
        action="help",
        help="show this help message and exit",
    )


def _write_project_readme(directory: Path, *, force: bool) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    output = directory / "README.md"
    mode = "w" if force else "x"
    try:
        with output.open(mode, encoding="utf-8", newline="\n") as stream:
            stream.write(PROJECT_README)
    except FileExistsError as error:
        raise FileExistsError(
            f"README already exists at '{output}'; use --force to replace it"
        ) from error
    return output


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="smoking-sbdf",
        description="Convert CSV, Parquet, and dataset inputs to SBDF.",
        epilog=(
            "LLM entrypoints: 'smoking-sbdf -help', "
            "'smoking-sbdf convert -help', and 'smoking-sbdf init -help'."
        ),
        add_help=False,
    )
    _add_help_argument(parser)
    parser.add_argument("--version", action="version", version=_package_version())
    commands = parser.add_subparsers(dest="command", required=True)
    convert_parser = commands.add_parser(
        "convert",
        help="convert one input into an SBDF file",
        add_help=False,
    )
    _add_help_argument(convert_parser)
    convert_parser.add_argument("input", type=Path, help="input file or directory")
    convert_parser.add_argument("output", type=Path, help="output .sbdf file")
    convert_parser.add_argument(
        "--input-format",
        default="auto",
        choices=(
            "auto",
            "csv",
            "parquet",
            "parquet-dataset",
            "parquet-manifest",
        ),
    )
    convert_parser.add_argument("--batch-size", type=int, default=5_000)
    convert_parser.add_argument(
        "--workers",
        type=_worker_count,
        metavar="1..8",
        help="maximum workers (default: CSV 1, Parquet 3)",
    )
    convert_parser.add_argument(
        "--column-type",
        action="append",
        default=[],
        metavar="COLUMN=SBDF_TYPE",
        help="override a column type; may be repeated",
    )
    convert_parser.add_argument(
        "--encoding-rle",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    convert_parser.add_argument("--adaptive-encoding", action="store_true")
    convert_parser.add_argument(
        "--adaptive-workers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "allow Parquet metadata to reduce --workers; "
            "use --no-adaptive-workers to force the requested count"
        ),
    )
    csv_options = convert_parser.add_argument_group("CSV options")
    csv_options.add_argument("--infer-schema-rows", type=int, default=10_000)
    csv_options.add_argument("--delimiter", default=",")
    csv_options.add_argument(
        "--header", action=argparse.BooleanOptionalAction, default=True
    )
    dataset_options = convert_parser.add_argument_group("Parquet dataset options")
    dataset_options.add_argument("--recursive", action="store_true")

    init_parser = commands.add_parser(
        "init",
        help="create an LLM-friendly README.md",
        add_help=False,
    )
    _add_help_argument(init_parser)
    init_parser.add_argument(
        "directory",
        nargs="?",
        type=Path,
        default=Path("."),
        help="directory where README.md will be created (default: current directory)",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="replace an existing README.md",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "init":
            output = _write_project_readme(
                arguments.directory,
                force=arguments.force,
            )
        else:
            column_types = _column_types(arguments.column_type)
            output = convert(
                arguments.input,
                arguments.output,
                input_format=arguments.input_format,
                batch_size=arguments.batch_size,
                column_types=column_types,
                encoding_rle=arguments.encoding_rle,
                adaptive_encoding=arguments.adaptive_encoding,
                workers=arguments.workers,
                adaptive_workers=arguments.adaptive_workers,
                infer_schema_rows=arguments.infer_schema_rows,
                delimiter=arguments.delimiter,
                has_header=arguments.header,
                recursive=arguments.recursive,
            )
    except (SBDFError, OSError, TypeError, ValueError) as error:
        print(f"smoking-sbdf: error: {error}", file=sys.stderr)
        return 2
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
