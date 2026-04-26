"""
Download CRDC flat files and crosswalk workbooks

Usage:
    python -m ingestion.download --source crdc --vintage 2021-22
    python -m ingestion.download --source crdc --all
    python -m ingestion.download --source crosswalk --vintage 2021-22
    python -m ingestion.download --source crosswalk --all
"""
import argparse
import zipfile
from pathlib import Path
import httpx
import logging

logging.basicConfig(
      level=logging.INFO,
      format="%(asctime)s [%(levelname)s] %(message)s",
      datefmt="%Y-%m-%d %H:%M:%S",                                                           
  )
logger = logging.getLogger(__name__)

VINTAGE_URLS = {
    "2021-22": "https://civilrightsdata.ed.gov/assets/ocr/docs/2021-22-crdc-data.zip",
    "2020-21": "https://civilrightsdata.ed.gov/assets/ocr/docs/2020-21-crdc-data.zip",
    "2017-18": "https://civilrightsdata.ed.gov/assets/ocr/docs/2017-18-crdc-data.zip",
}

CROSSWALK_URLS = {
    "2021-22": "https://civilrightsdata.ed.gov/assets/downloads/2021-22%20Appendix%20Workbook.xlsx",
    "2020-21": "https://civilrightsdata.ed.gov/assets/downloads/2020-21%20Appendix%20Workbook.xlsx",
}

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
CROSSWALK_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "crosswalk"

def _download_file(url: str, dest_path: Path) -> None:
    tmp_path = dest_path.with_name(dest_path.name + ".tmp")                                              
    try:
        with httpx.Client(follow_redirects=True, timeout=300) as client:
            with client.stream("GET", url) as response:
                response.raise_for_status()
                total = int(response.headers.get("content-length", 0))
                downloaded = 0
                with open(tmp_path, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            pct = downloaded / total * 100
                            print(f"\r  {downloaded / 1e6:.1f} MB / {total / 1e6:.1f} MB ({pct:.0f}%)", end="", flush=True)
                print()
        tmp_path.rename(dest_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()                                                                            
        raise


def download_vintage(vintage: str) -> None:
    url = VINTAGE_URLS[vintage]
    zip_path = DATA_DIR / f"{vintage}.zip"                                                 
    extract_dir = DATA_DIR / vintage
                                                                                             
    DATA_DIR.mkdir(exist_ok=True)

    if extract_dir.exists():
        logger.info(f"[{vintage}] Already extracted, skipping.")
        return                                                                             
   
    if zip_path.exists():                                                                  
        logger.info(f"[{vintage}] ZIP already exists, skipping download.")
    else:                                                                                  
        logger.info(f"[{vintage}] Downloading from {url} ...")
        _download_file(url, zip_path)
        logger.info(f"[{vintage}] Download complete.")
    
    logger.info(f"[{vintage}] Extracting to {extract_dir} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)                                                         
    logger.info(f"[{vintage}] Extraction complete.")
    zip_path.unlink()
    logger.info(f"[{vintage}] Zip deleted.")


def download_crosswalk(vintage: str) -> None:
    url = CROSSWALK_URLS[vintage]
    dest = CROSSWALK_DIR / f"{vintage}-appendix.xlsx"
    
    CROSSWALK_DIR.mkdir(exist_ok=True)
    
    if dest.exists():
        logger.info(f"[{vintage}] Crosswalk already downloaded, skipping.")
        return
    
    logger.info(f"[{vintage}] Downloading crosswalk from {url} ...")
    _download_file(url, dest)
    logger.info(f"[{vintage}] Crosswalk download complete.")


def main():
    parser = argparse.ArgumentParser(description="Download CRDC flat files and crosswalk workbooks.")
    parser.add_argument("--source", choices=["crdc", "crosswalk"], required=True,
                        help="Which data source to download")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--vintage", help="Single vintage to download")
    group.add_argument("--all", action="store_true", help="Download all vintages for the selected source")
    args = parser.parse_args()
    
    if args.source == "crdc":
        valid = list(VINTAGE_URLS.keys())
        vintages = valid if args.all else [args.vintage]
        for v in vintages:
            if v not in valid:
                parser.error(f"Invalid vintage {v!r} for --source crdc. Choose from: {valid}")
            download_vintage(v)                                                  
                  
    elif args.source == "crosswalk":
        valid = list(CROSSWALK_URLS.keys())
        vintages = valid if args.all else [args.vintage]
        for v in vintages:
            if v not in valid:
                parser.error(f"Invalid vintage {v!r} for --source crosswalk. Choose from: {valid}")
            download_crosswalk(v)

                                                                                     
if __name__ == "__main__":                                                                 
    main()