import sys
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

def is_zip_valid(path: str) -> bool:
    import zipfile

    try:
        with zipfile.ZipFile(path, "r") as z:
            return z.testzip() is None
    except Exception:
        return False


def download_files(url_file: str, path_destiny: str, **context):
    """
    Download all .zip files from a given URL into path_destiny.
    Safe for large files (1GB+), Docker and Airflow retries.
    """
    import os
    from urllib.parse import urljoin
    import requests
    from bs4 import BeautifulSoup
    import shutil

    os.makedirs(path_destiny, exist_ok=True)
    logger.info("Starting HTML request: %s", url_file)

    response_request = requests.get(url_file, timeout=30)
    response_request.raise_for_status()

    soup = BeautifulSoup(response_request.text, "html.parser")
    archive_names = [
        a["href"]
        for a in soup.find_all("a")
        if a.get("href", "").endswith(".zip")
    ]

    logger.info("Files found on page: %d", len(archive_names))

    for archive in archive_names:
        local_path = os.path.join(path_destiny, archive)

        if os.path.exists(local_path) and is_zip_valid(local_path):
            logger.info("File already exists and is valid: %s", archive)
            continue

        if os.path.exists(local_path):
            logger.warning("Removing corrupted file: %s", archive)
            os.remove(local_path)

        file_url = urljoin(url_file, archive)
        tmp_path = local_path + ".part"

        logger.info("Downloading file: %s -> %s", file_url, local_path)

        try:
            with requests.get(
                file_url,
                stream=True,
                timeout=(10, 900)
            ) as r:
                r.raise_for_status()

                expected_size = int(r.headers.get("Content-Length", 0))

                with open(tmp_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)

                downloaded = os.path.getsize(tmp_path)

                if expected_size and downloaded != expected_size:
                    raise IOError(
                        f"Incomplete download for {archive}: "
                        f"{downloaded} / {expected_size} bytes"
                    )

            os.rename(tmp_path, local_path)

            if not is_zip_valid(local_path):
                raise IOError(f"ZIP corrupted after download: {archive}")

            logger.info("Download completed successfully: %s", archive)

        except Exception:
            logger.exception("Failed to download file: %s", archive)
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise


def download_files_for_period(
    origin_base_path: str,
    date: str,
    **context):
    """
    Airflow PythonOperator callable.
    Uses forced_date if provided, otherwise takes data_interval_start from context.
    Date in format YYYY-MM
    """
    from datetime import datetime

    import os
    from urllib.parse import urljoin


    base_url = os.getenv("RFB_BASE_URL")

    if not base_url:
        raise RuntimeError("RFB_BASE_URL not set in environment")

    dt = datetime.strptime(date, "%Y-%m")
    year = dt.year
    month = dt.month

    local_path = os.path.join(origin_base_path, f"{year}-{month:02d}")
    url = urljoin(base_url.rstrip("/") + "/", f"{year}-{month:02d}/")

    logger.info(
        "Downloading period year=%s month=%s to %s from %s",
        year,
        month,
        local_path,
        url,
    )

    download_files(url, local_path, **context)


def download_files_for_range(
    origin_base_path:str,
    start_date: str,
    end_date: str,
    **context ):
    """
    Download files for all months between start_date and end_date.
    start_date and end_date must be in format YYYY-MM
    """
    import pendulum

    start = pendulum.from_format(start_date, "YYYY-MM").start_of("month")
    end = pendulum.from_format(end_date, "YYYY-MM").start_of("month")


    current = start

    while current <= end:
        date_format = current.format("YYYY-MM")

        logger.info("Processing month: %s", date_format)

        download_files_for_period(
            origin_base_path=origin_base_path,
            date=date_format,
            **context
        )
        current = current.add(months=1)