import os
import sys
from pathlib import Path
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)


def check_path(path: str) -> None:
    """
    Check if the path exists and is a directory.
    """
    path = Path(path)

    if not path.exists():
        logger.error("Path does not exist: %s", path)
        raise FileNotFoundError(f"The path '{path}' does not exist")

    if not path.is_dir():
        logger.error("Path is not a directory: %s", path)
        raise NotADirectoryError(f"The path '{path}' is not a directory")


def list_archives(path: str,  **context) -> list[Path]:
    """
    Return a list of .zip files found in the given directory.
    """
    check_path(path)

    path = Path(path)
    zip_files = [
        p for p in path.iterdir()
        if p.is_file() and p.suffix.lower() == ".zip"
    ]

    if not zip_files:
        logger.warning("No .zip files found in directory: %s", path)
        raise FileNotFoundError("No .zip file found in the directory")

    logger.info("Found %d zip file(s) in %s", len(zip_files), path)
    return zip_files


def uncompress_zip_file(origin_path: str, output_dir: str, **context) -> None:
    """
    Extract all .zip files from the source directory into the output directory.
    """
    import shutil
    import zipfile
    from zipfile import ZipFile

    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    zip_files = list_archives(origin_path)

    for file in zip_files:
        try:
            logger.info("Extracting zip file: %s", file)

            with ZipFile(file, "r") as zip_obj:
                for member in zip_obj.infolist():

                    name = Path(member.filename).name
                    target_path = output_dir / name

                    if member.is_dir():
                        if target_path.exists():
                            logger.info(
                                "Skipping directory. Already exists: %s",
                                target_path
                            )
                        else:
                            target_path.mkdir()
                        continue

                    if target_path.exists():
                        logger.info(
                            "Skipping extraction. File already exists: %s",
                            target_path
                        )
                        continue

                    with zip_obj.open(member) as source, open(target_path, "wb") as target:
                        shutil.copyfileobj(source, target)

        except zipfile.BadZipFile:
            logger.exception("Corrupted ZIP file skipped: %s", file)


def uncompress_zip_file_range(
    origin_base_path: str,
    output_dir: str,
    start_date: str,
    end_date: str,
    **context,
) -> None:
    """
    Uncompress zip files month by month within a date range.
    Supports folders in the format YYYY-MM.
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")
    current = start

    processed_months = 0
    while current <= end:
        origin_month_path = Path(origin_base_path) / f"{current.year}-{current.month:02d}"

        output_month_path = Path(output_dir) / f"{current.year}-{current.month:02d}"

        logger.info(
            "Processing month %s",
            current.strftime("%Y-%m")
        )

        try:
            uncompress_zip_file(
                origin_path=str(origin_month_path),
                output_dir=str(output_month_path),
            )
            processed_months += 1

        except FileNotFoundError:
            logger.warning(
                "No files found for %s",
                current.strftime("%Y-%m")
            )

        current += relativedelta(months=1)

    if processed_months == 0:
        raise RuntimeWarning("No zip files were processed in the given date range")


def unzip_zip_to_parquet(origin_path: str, output_dir: str, sep=";", **context) -> None:
    """
    Convert all .zip files from the source directory into parquet files.
    """
    # import pandas as pd
    import pyarrow as pa
    import pyarrow.csv as pv
    import pyarrow.parquet as pq
    import zipfile
    from zipfile import ZipFile

    output_dir = Path(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    zip_files = list_archives(origin_path)

    for file in zip_files:
        try:
            logger.info("Processing zip file: %s", file)

            with ZipFile(file, "r") as zip_obj:
                for member in zip_obj.infolist():

                    path = Path(member.filename)
                    name = path.name

                    steam = path.stem
                    suffix = path.suffix.lstrip(".")
                    base_suffix = suffix.removesuffix("CSV")

                    parquet_path = output_dir / f"{steam}_{base_suffix}_{file.stem}.parquet"

                    if parquet_path.exists():
                        logger.info(
                            "Skipping parquet. Already exists: %s",
                            parquet_path
                        )
                        continue

                    logger.info(
                        "Converting %s -> %s",
                        name,
                        parquet_path.name
                    )

                    with zip_obj.open(member) as f:
                        csv_file = pv.open_csv(
                            f,
                            read_options=pv.ReadOptions(
                                block_size= 8 * 1024 * 1024,
                                encoding="latin1"
                            ),
                            parse_options=pv.ParseOptions(
                                delimiter=sep,
                                quote_char='"'
                            ),
                            convert_options=pv.ConvertOptions(
                                strings_can_be_null=True,
                                null_values=["", "NULL"]
                            )
                        )

                        writer = None

                        for batch in csv_file:
                            table = pa.Table.from_batches([batch])

                            if writer is None:
                                writer = pq.ParquetWriter(
                                    parquet_path,
                                    table.schema,
                                    compression="snappy",
                                    write_statistics=True,
                                    use_dictionary=True
                                    )

                            writer.write_table(table, row_group_size=10_000)

                        if writer:
                            writer.close()

                        if writer is None:
                            logger.warning("CSV vazio ou sem dados: %s", name)


        except zipfile.BadZipFile:
            logger.exception("Corrupted ZIP file skipped: %s", file)


def unzip_zip_to_parquet_range(
    origin_base_path: str,
    output_dir: str,
    start_date: str,
    end_date: str,
    sep=";",
    **context,
) -> None:
    """
    Convert zip files to parquet month by month within a date range.
    Supports folders in the format YYYY-MM.
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    from pathlib import Path

    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")
    current = start

    processed_months = 0

    while current <= end:
        origin_month_path = Path(origin_base_path) / f"{current.year}-{current.month:02d}"
        output_month_path = Path(output_dir) / f"{current.year}-{current.month:02d}"

        logger.info(
            "Processing month %s",
            current.strftime("%Y-%m")
        )

        try:
            unzip_zip_to_parquet(
                origin_path=str(origin_month_path),
                output_dir=str(output_month_path),
                sep=sep,
            )
            processed_months += 1

        except FileNotFoundError:
            logger.warning(
                "No files found for %s",
                current.strftime("%Y-%m")
            )

        except Exception as e:
            logger.exception(
                "Error processing the month %s: %s", current.strftime("%Y-%m"), e
            )

        current += relativedelta(months=1)

    if processed_months == 0:
        raise RuntimeWarning("No zip files were processed in the given date range")


def list_archives_general(path: str,  **context) -> list[Path]:
    """
    Return a list files found in the given directory.
    """
    check_path(path)

    path = Path(path)
    zip_files = [
        p for p in path.iterdir() if p.is_file()
    ]

    if not zip_files:
        logger.warning("No files found in directory: %s", path)
        raise FileNotFoundError("No files found in the directory")

    logger.info("Found %d zip file(s) in %s", len(zip_files), path)
    return zip_files


def csv_to_parquet(origin_path: str, output_dir: str, sep=";", **context) -> None:
    import pyarrow as pa
    import pyarrow.csv as pv
    import pyarrow.parquet as pq

    origin_path = Path(origin_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = list_archives_general(str(origin_path))

    for file in files:
        if file.suffix.lower() != ".csv":
            logger.info("Skipping non-CSV file: %s", file.name)
            continue

        parquet_path = output_dir / f"{file.stem}.parquet"

        if parquet_path.exists():
            logger.info("Parquet already exists, skipping: %s", parquet_path.name)
            continue

        logger.info("Converting CSV -> Parquet: %s", file.name)

        try:
            csv = pv.open_csv(
                file,
                read_options=pv.ReadOptions(
                    block_size=8 * 1024 * 1024,
                    encoding="latin1",
                ),
                parse_options=pv.ParseOptions(delimiter=sep),
            )

            writer = None

            for batch in csv:
                table = pa.Table.from_batches([batch])

                if writer is None:
                    writer = pq.ParquetWriter(
                        parquet_path,
                        table.schema,
                        compression="snappy",
                        use_dictionary=True,
                    )

                writer.write_table(table)

            if writer:
                writer.close()
            else:
                logger.warning("CSV vazio: %s", file.name)

        except Exception:
            logger.exception("Failed converting CSV: %s", file.name)
            if parquet_path.exists():
                parquet_path.unlink()
            raise


def csv_to_parquet_range(
    origin_base_path: str,
    output_dir: str,
    start_date: str,
    end_date: str,
    sep=";",
    **context,
) -> None:
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")
    current = start

    while current <= end:
        month = current.strftime("%Y-%m")

        logger.info("Processing month: %s", month)

        try:
            csv_to_parquet(
                origin_path=f"{origin_base_path}/{month}",
                output_dir=f"{output_dir}/{month}",
                sep=sep,
            )
        except FileNotFoundError:
            logger.warning("No files found for %s", month)

        current += relativedelta(months=1)