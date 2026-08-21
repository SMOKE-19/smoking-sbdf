"""Rust-backed SBDF writers for Parquet, CSV, and Python DataFrames."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
from importlib import import_module
from importlib.util import find_spec
import math
from pathlib import Path
from typing import Literal

from ._native import (
    SBDFError,
    StreamingSbdfWriter,
    csv_to_sbdf_streaming as _csv_to_sbdf_streaming,
    generate_sbdf_sidecar as _generate_sbdf_sidecar,
    parquet_files_to_sbdf_streaming as _parquet_files_to_sbdf_streaming,
    parquet_to_sbdf_streaming as _parquet_to_sbdf_streaming,
    resolve_dataframe_column_types as _resolve_dataframe_column_types,
)

InputFormat = Literal[
    "auto", "csv", "parquet", "parquet-dataset", "parquet-manifest"
]
MIN_WORKERS = 1
MAX_WORKERS = 8


def _validate_workers(workers: int) -> int:
    if isinstance(workers, bool) or not isinstance(workers, int):
        raise TypeError("workers must be an integer")
    if not MIN_WORKERS <= workers <= MAX_WORKERS:
        raise ValueError(
            f"workers must be between {MIN_WORKERS} and {MAX_WORKERS}"
        )
    return workers


def sbdf_sidecar_path(sbdf_path: str | Path) -> Path:
    """Return the default Parquet sidecar path for an SBDF file."""
    return Path(f"{Path(sbdf_path)}.sidecar.parquet")


def generate_sbdf_sidecar(
    sbdf_path: str | Path,
    *,
    row_key_columns: Sequence[str],
    sidecar_path: str | Path | None = None,
    table_id: str | None = None,
) -> Path:
    """Create a row-key and slice-coordinate Parquet sidecar for an SBDF file."""
    sbdf_path = Path(sbdf_path)
    if isinstance(row_key_columns, (str, bytes)):
        raise TypeError("row_key_columns must be a sequence of column names")
    keys = list(row_key_columns)
    if not keys:
        raise ValueError("row_key_columns must not be empty")
    if any(not isinstance(column, str) or not column for column in keys):
        raise TypeError("row_key_columns must contain non-empty strings")
    if len(set(keys)) != len(keys):
        raise ValueError("row_key_columns must not contain duplicates")
    resolved_sidecar_path = (
        sbdf_sidecar_path(sbdf_path)
        if sidecar_path is None
        else Path(sidecar_path)
    )
    resolved_table_id = sbdf_path.stem if table_id is None else table_id
    if not isinstance(resolved_table_id, str) or not resolved_table_id.strip():
        raise ValueError("table_id must be a non-empty string")
    resolved_sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    _generate_sbdf_sidecar(
        str(sbdf_path),
        str(resolved_sidecar_path),
        resolved_table_id,
        keys,
    )
    return resolved_sidecar_path


def _generate_requested_sidecar(
    sbdf_path: Path,
    row_key_columns: Sequence[str] | None,
    sidecar_path: str | Path | None,
    table_id: str | None,
) -> None:
    if row_key_columns is None:
        if sidecar_path is not None or table_id is not None:
            raise ValueError(
                "sidecar_path and table_id require row_key_columns"
            )
        return
    resolved_sidecar_path = (
        sbdf_sidecar_path(sbdf_path)
        if sidecar_path is None
        else Path(sidecar_path)
    )
    try:
        generate_sbdf_sidecar(
            sbdf_path,
            row_key_columns=row_key_columns,
            sidecar_path=resolved_sidecar_path,
            table_id=table_id,
        )
    except Exception:
        # The SBDF has already been replaced successfully. Never leave an older
        # sidecar that now points at unrelated byte ranges.
        resolved_sidecar_path.unlink(missing_ok=True)
        raise


def _validate_requested_sidecar(
    sbdf_path: str | Path,
    row_key_columns: Sequence[str] | None,
    sidecar_path: str | Path | None,
    table_id: str | None,
) -> None:
    if row_key_columns is None:
        if sidecar_path is not None or table_id is not None:
            raise ValueError("sidecar_path and table_id require row_key_columns")
        return
    if isinstance(row_key_columns, (str, bytes)):
        raise TypeError("row_key_columns must be a sequence of column names")
    keys = list(row_key_columns)
    if not keys:
        raise ValueError("row_key_columns must not be empty")
    if any(not isinstance(column, str) or not column for column in keys):
        raise TypeError("row_key_columns must contain non-empty strings")
    if len(set(keys)) != len(keys):
        raise ValueError("row_key_columns must not contain duplicates")
    resolved_sidecar_path = (
        sbdf_sidecar_path(sbdf_path)
        if sidecar_path is None
        else Path(sidecar_path)
    )
    if Path(sbdf_path) == resolved_sidecar_path:
        raise ValueError("sidecar_path must differ from sbdf_path")
    if table_id is not None and (not isinstance(table_id, str) or not table_id.strip()):
        raise ValueError("table_id must be a non-empty string")


def csv_to_sbdf_streaming(
    csv_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 5_000,
    infer_schema_rows: int = 10_000,
    column_types: dict[str, str] | None = None,
    delimiter: str | bytes = ",",
    has_header: bool = True,
    encoding_rle: bool = True,
    adaptive_encoding: bool = False,
    workers: int = 1,
    row_key_columns: Sequence[str] | None = None,
    sidecar_path: str | Path | None = None,
    table_id: str | None = None,
) -> Path:
    """Stream CSV into SBDF with 1–8 workers; one worker is the default."""
    workers = _validate_workers(workers)
    _validate_requested_sidecar(
        sbdf_path, row_key_columns, sidecar_path, table_id
    )
    if isinstance(delimiter, str):
        encoded_delimiter = delimiter.encode("utf-8")
    elif isinstance(delimiter, bytes):
        encoded_delimiter = delimiter
    else:
        raise TypeError("delimiter must be one str or bytes character")
    if len(encoded_delimiter) != 1:
        raise ValueError("delimiter must encode to exactly one byte")

    csv_path = Path(csv_path)
    sbdf_path = Path(sbdf_path)
    sbdf_path.parent.mkdir(parents=True, exist_ok=True)
    _csv_to_sbdf_streaming(
        str(csv_path),
        str(sbdf_path),
        batch_size=batch_size,
        infer_schema_rows=infer_schema_rows,
        column_types=column_types,
        delimiter=encoded_delimiter[0],
        has_header=has_header,
        encoding_rle=encoding_rle,
        adaptive_encoding=adaptive_encoding,
        workers=workers,
    )
    _generate_requested_sidecar(
        sbdf_path, row_key_columns, sidecar_path, table_id
    )
    return sbdf_path


def _dataframe_batch(dataframe: object) -> tuple[list[str], list[str], dict[str, list]]:
    try:
        original_columns = list(dataframe.columns)  # type: ignore[attr-defined]
        dtype_names = [str(dtype) for dtype in dataframe.dtypes]  # type: ignore[attr-defined]
    except AttributeError as error:
        raise TypeError("dataframe must expose columns, dtypes, and to_dict") from error

    columns = [str(column) for column in original_columns]
    if len(set(columns)) != len(columns):
        raise ValueError("DataFrame column names must be unique after string conversion")

    module_root = type(dataframe).__module__.split(".", 1)[0]
    if module_root == "pandas":
        raw_batch = dataframe.to_dict(orient="list")  # type: ignore[attr-defined]
    elif module_root == "polars":
        raw_batch = dataframe.to_dict(as_series=False)  # type: ignore[attr-defined]
    else:
        try:
            raw_batch = dataframe.to_dict(orient="list")  # type: ignore[attr-defined]
        except TypeError:
            raw_batch = dataframe.to_dict(as_series=False)  # type: ignore[attr-defined]

    if not isinstance(raw_batch, dict):
        raise TypeError("dataframe.to_dict() must return a column-oriented dict")
    batch = {str(column): list(values) for column, values in raw_batch.items()}
    if list(batch) != columns:
        raise ValueError("DataFrame columns changed while creating the SBDF batch")
    if len(dtype_names) != len(columns):
        raise ValueError("DataFrame columns and dtypes have different lengths")
    return columns, dtype_names, batch


def _is_missing_value(value: object) -> bool:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return True
    value_type = type(value)
    return value_type.__module__.startswith("pandas.") and value_type.__name__ in {
        "NAType",
        "NaTType",
    }


def _python_dtype_name(value: object) -> str | None:
    if _is_missing_value(value):
        return None
    if isinstance(value, bool):
        return "python:bool"
    if isinstance(value, datetime):
        return "python:datetime"
    if isinstance(value, timedelta):
        return "python:timedelta"
    if isinstance(value, time):
        return "python:time"
    if isinstance(value, date):
        return "python:date"
    if isinstance(value, int):
        return "python:int"
    if isinstance(value, float):
        return "python:float"
    if isinstance(value, str):
        return "python:str"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "python:bytes"
    return None


def _refine_object_dtypes(dtype_names: list[str], batch: dict[str, list]) -> list[str]:
    refined = list(dtype_names)
    for index, (column, dtype_name) in enumerate(zip(batch, dtype_names, strict=True)):
        if dtype_name.strip().lower() != "object":
            continue
        for value in batch[column]:
            inferred = _python_dtype_name(value)
            if inferred is not None:
                refined[index] = inferred
                break
    return refined


def _coerce_value(value: object, sbdf_type: str) -> object:
    if _is_missing_value(value):
        return None
    if sbdf_type == "Boolean":
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1"}:
                return True
            if normalized in {"false", "0"}:
                return False
            raise ValueError(f"cannot convert {value!r} to Boolean")
        return bool(value)
    if sbdf_type in {"Integer", "LongInteger"}:
        return int(value)  # type: ignore[arg-type]
    if sbdf_type in {"SingleReal", "Real"}:
        return float(value)  # type: ignore[arg-type]
    if sbdf_type == "DateTime":
        converted = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
        if converted.tzinfo is not None:
            converted = converted.astimezone(timezone.utc).replace(tzinfo=None)
        return converted
    if sbdf_type == "Date":
        if isinstance(value, datetime):
            return value.date()
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    if sbdf_type == "Time":
        return value if isinstance(value, time) else time.fromisoformat(str(value))
    if sbdf_type == "TimeSpan":
        if isinstance(value, timedelta):
            return value
        raise TypeError(f"cannot convert {value!r} to TimeSpan")
    if sbdf_type == "String":
        return str(value)
    if sbdf_type == "Binary":
        return value.encode() if isinstance(value, str) else bytes(value)  # type: ignore[arg-type]
    raise ValueError(f"unknown Spotfire type: {sbdf_type}")


def dataframe_to_sbdf(
    dataframe: object,
    sbdf_path: str | Path,
    *,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
    adaptive_encoding: bool = False,
    row_key_columns: Sequence[str] | None = None,
    sidecar_path: str | Path | None = None,
    table_id: str | None = None,
) -> Path:
    """Write a pandas/Polars-style DataFrame to SBDF in one in-memory batch."""
    _validate_requested_sidecar(
        sbdf_path, row_key_columns, sidecar_path, table_id
    )
    columns, dtype_names, batch = _dataframe_batch(dataframe)
    dtype_names = _refine_object_dtypes(dtype_names, batch)
    resolved_types = _resolve_dataframe_column_types(columns, dtype_names)

    if column_types:
        unknown = sorted(set(column_types) - set(columns))
        if unknown:
            raise ValueError(f"column_types contains unknown columns: {unknown}")
        resolved_types.update(column_types)

    for column in columns:
        batch[column] = [
            _coerce_value(value, resolved_types[column]) for value in batch[column]
        ]
    sbdf_path = Path(sbdf_path)
    sbdf_path.parent.mkdir(parents=True, exist_ok=True)
    writer = StreamingSbdfWriter(
        str(sbdf_path),
        columns=columns,
        column_types=resolved_types,
        encoding_rle=encoding_rle,
        adaptive_encoding=adaptive_encoding,
    )
    try:
        writer.write_batch(batch)
    finally:
        writer.close()
    _generate_requested_sidecar(
        sbdf_path, row_key_columns, sidecar_path, table_id
    )
    return sbdf_path


def _dataframe_to_sbdf_method(
    self: object,
    path_or_buf: str | Path,
    *,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
    adaptive_encoding: bool = False,
    row_key_columns: Sequence[str] | None = None,
    sidecar_path: str | Path | None = None,
    table_id: str | None = None,
) -> Path:
    """Write this DataFrame to an SBDF file."""
    return dataframe_to_sbdf(
        self,
        path_or_buf,
        column_types=column_types,
        encoding_rle=encoding_rle,
        adaptive_encoding=adaptive_encoding,
        row_key_columns=row_key_columns,
        sidecar_path=sidecar_path,
        table_id=table_id,
    )


def register_dataframe_type(
    dataframe_type: type,
    *,
    overwrite: bool = False,
) -> bool:
    """Register ``DataFrame.to_sbdf`` on one DataFrame-compatible class.

    Returns ``True`` when the method was installed. Existing methods are left
    untouched unless ``overwrite=True``.
    """
    if not isinstance(dataframe_type, type):
        raise TypeError("dataframe_type must be a class")
    existing = dataframe_type.__dict__.get("to_sbdf")
    if existing is _dataframe_to_sbdf_method:
        return False
    if existing is not None and not overwrite:
        return False
    setattr(dataframe_type, "to_sbdf", _dataframe_to_sbdf_method)
    return True


def install_dataframe_methods(*, overwrite: bool = False) -> tuple[str, ...]:
    """Install ``to_sbdf`` on available pandas and Polars DataFrame classes."""
    installed = []
    for module_name in ("pandas", "polars"):
        if find_spec(module_name) is None:
            continue
        module = import_module(module_name)
        dataframe_type = getattr(module, "DataFrame", None)
        if isinstance(dataframe_type, type) and register_dataframe_type(
            dataframe_type, overwrite=overwrite
        ):
            installed.append(module_name)
    return tuple(installed)


to_sbdf = dataframe_to_sbdf


def parquet_to_sbdf_streaming(
    parquet_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 5_000,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
    adaptive_encoding: bool = False,
    workers: int = 3,
    adaptive_workers: bool = True,
    row_key_columns: Sequence[str] | None = None,
    sidecar_path: str | Path | None = None,
    table_id: str | None = None,
) -> Path:
    """Stream Parquet with 1–8 workers and optional adaptive reduction."""
    workers = _validate_workers(workers)
    _validate_requested_sidecar(
        sbdf_path, row_key_columns, sidecar_path, table_id
    )
    parquet_path = Path(parquet_path)
    sbdf_path = Path(sbdf_path)
    sbdf_path.parent.mkdir(parents=True, exist_ok=True)
    _parquet_to_sbdf_streaming(
        str(parquet_path),
        str(sbdf_path),
        batch_size=batch_size,
        column_types=column_types,
        encoding_rle=encoding_rle,
        adaptive_encoding=adaptive_encoding,
        workers=workers,
        adaptive_workers=adaptive_workers,
    )
    _generate_requested_sidecar(
        sbdf_path, row_key_columns, sidecar_path, table_id
    )
    return sbdf_path


def parquet_files_to_sbdf_streaming(
    parquet_files: list[str | Path],
    sbdf_path: str | Path,
    *,
    batch_size: int = 5_000,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
    adaptive_encoding: bool = False,
    workers: int = 3,
    adaptive_workers: bool = True,
    row_key_columns: Sequence[str] | None = None,
    sidecar_path: str | Path | None = None,
    table_id: str | None = None,
) -> Path:
    """Stream Parquet files with 1–8 workers into one SBDF table."""
    workers = _validate_workers(workers)
    _validate_requested_sidecar(
        sbdf_path, row_key_columns, sidecar_path, table_id
    )
    parquet_files = [Path(path) for path in parquet_files]
    sbdf_path = Path(sbdf_path)
    sbdf_path.parent.mkdir(parents=True, exist_ok=True)
    _parquet_files_to_sbdf_streaming(
        [str(path) for path in parquet_files],
        str(sbdf_path),
        batch_size=batch_size,
        column_types=column_types,
        encoding_rle=encoding_rle,
        adaptive_encoding=adaptive_encoding,
        workers=workers,
        adaptive_workers=adaptive_workers,
    )
    _generate_requested_sidecar(
        sbdf_path, row_key_columns, sidecar_path, table_id
    )
    return sbdf_path


def parquet_dataset_to_sbdf_streaming(
    dataset_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 5_000,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
    adaptive_encoding: bool = False,
    recursive: bool = False,
    workers: int = 3,
    adaptive_workers: bool = True,
    row_key_columns: Sequence[str] | None = None,
    sidecar_path: str | Path | None = None,
    table_id: str | None = None,
) -> Path:
    """Stream all .parquet files from a dataset directory into one SBDF table."""
    dataset_path = Path(dataset_path)
    if not dataset_path.is_dir():
        raise ValueError(f"dataset_path is not a directory: {dataset_path}")
    pattern = "**/*.parquet" if recursive else "*.parquet"
    parquet_files = sorted(dataset_path.glob(pattern))
    return parquet_files_to_sbdf_streaming(
        parquet_files,
        sbdf_path,
        batch_size=batch_size,
        column_types=column_types,
        encoding_rle=encoding_rle,
        adaptive_encoding=adaptive_encoding,
        workers=workers,
        adaptive_workers=adaptive_workers,
        row_key_columns=row_key_columns,
        sidecar_path=sidecar_path,
        table_id=table_id,
    )


def parquet_manifest_to_sbdf_streaming(
    manifest_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 5_000,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
    adaptive_encoding: bool = False,
    workers: int = 3,
    adaptive_workers: bool = True,
    row_key_columns: Sequence[str] | None = None,
    sidecar_path: str | Path | None = None,
    table_id: str | None = None,
) -> Path:
    """Stream Parquet files listed in a line-based manifest into one SBDF table."""
    manifest_path = Path(manifest_path)
    base_path = manifest_path.parent
    parquet_files = []
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        path = Path(entry)
        parquet_files.append(path if path.is_absolute() else base_path / path)
    return parquet_files_to_sbdf_streaming(
        parquet_files,
        sbdf_path,
        batch_size=batch_size,
        column_types=column_types,
        encoding_rle=encoding_rle,
        adaptive_encoding=adaptive_encoding,
        workers=workers,
        adaptive_workers=adaptive_workers,
        row_key_columns=row_key_columns,
        sidecar_path=sidecar_path,
        table_id=table_id,
    )


def _resolve_input_format(input_path: Path, input_format: str) -> str:
    normalized = input_format.strip().lower().replace("_", "-")
    aliases = {
        "dataset": "parquet-dataset",
        "manifest": "parquet-manifest",
    }
    normalized = aliases.get(normalized, normalized)
    supported = {
        "auto",
        "csv",
        "parquet",
        "parquet-dataset",
        "parquet-manifest",
    }
    if normalized not in supported:
        raise ValueError(
            f"unsupported input_format {input_format!r}; expected one of "
            f"{sorted(supported)}"
        )
    if normalized != "auto":
        return normalized
    if input_path.is_dir():
        return "parquet-dataset"
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".parquet", ".pq"}:
        return "parquet"
    if suffix == ".manifest":
        return "parquet-manifest"
    raise ValueError(
        f"cannot infer input format from {input_path}; use input_format explicitly"
    )


def convert(
    input_path: str | Path,
    sbdf_path: str | Path,
    *,
    input_format: InputFormat | str = "auto",
    batch_size: int = 5_000,
    column_types: dict[str, str] | None = None,
    encoding_rle: bool = True,
    adaptive_encoding: bool = False,
    workers: int | None = None,
    adaptive_workers: bool = True,
    infer_schema_rows: int = 10_000,
    delimiter: str | bytes = ",",
    has_header: bool = True,
    recursive: bool = False,
    row_key_columns: Sequence[str] | None = None,
    sidecar_path: str | Path | None = None,
    table_id: str | None = None,
) -> Path:
    """Convert a CSV or Parquet input into one SBDF file.

    ``input_format="auto"`` recognizes CSV and Parquet file suffixes, Parquet
    dataset directories, and ``.manifest`` files. ``workers`` accepts 1–8.
    CSV defaults to one worker; Parquet defaults to an adaptive maximum of
    three. Set ``adaptive_workers=False`` to force the requested Parquet count.
    """
    source = Path(input_path)
    resolved_format = _resolve_input_format(source, input_format)
    if resolved_format == "csv":
        return csv_to_sbdf_streaming(
            source,
            sbdf_path,
            batch_size=batch_size,
            infer_schema_rows=infer_schema_rows,
            column_types=column_types,
            delimiter=delimiter,
            has_header=has_header,
            encoding_rle=encoding_rle,
            adaptive_encoding=adaptive_encoding,
            workers=1 if workers is None else workers,
            row_key_columns=row_key_columns,
            sidecar_path=sidecar_path,
            table_id=table_id,
        )

    parquet_options = {
        "batch_size": batch_size,
        "column_types": column_types,
        "encoding_rle": encoding_rle,
        "adaptive_encoding": adaptive_encoding,
        "workers": 3 if workers is None else workers,
        "adaptive_workers": adaptive_workers,
        "row_key_columns": row_key_columns,
        "sidecar_path": sidecar_path,
        "table_id": table_id,
    }
    if resolved_format == "parquet":
        return parquet_to_sbdf_streaming(source, sbdf_path, **parquet_options)
    if resolved_format == "parquet-dataset":
        return parquet_dataset_to_sbdf_streaming(
            source,
            sbdf_path,
            recursive=recursive,
            **parquet_options,
        )
    return parquet_manifest_to_sbdf_streaming(source, sbdf_path, **parquet_options)


__all__ = [
    "SBDFError",
    "StreamingSbdfWriter",
    "convert",
    "csv_to_sbdf_streaming",
    "dataframe_to_sbdf",
    "generate_sbdf_sidecar",
    "install_dataframe_methods",
    "parquet_dataset_to_sbdf_streaming",
    "parquet_files_to_sbdf_streaming",
    "parquet_manifest_to_sbdf_streaming",
    "parquet_to_sbdf_streaming",
    "register_dataframe_type",
    "sbdf_sidecar_path",
    "to_sbdf",
]


install_dataframe_methods()
