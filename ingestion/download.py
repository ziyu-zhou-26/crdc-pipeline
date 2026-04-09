"""
Download and extract CRDC flat files from the OCR portal

Usage:
    python ingestion/download.py --vintage 2021-22
    python ingestion/download.py --all
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

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

def download_vintage(vintage: str, keep_zip: bool = False) -> None:
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
        tmp_path = zip_path.with_suffix(".zip.tmp")
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
            tmp_path.rename(zip_path)                                                      
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        logger.info(f"[{vintage}] Download complete.")                                             
                  
    logger.info(f"[{vintage}] Extracting to {extract_dir} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)                                                         
    logger.info(f"[{vintage}] Extraction complete.")
    if not keep_zip:
        zip_path.unlink()
        logger.info(f"[{vintage}] Zip deleted.")
                                                                                             
                  
def main():
    parser = argparse.ArgumentParser(description="Download CRDC flat files from the OCR portal.")                                                                                  
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--vintage", choices=list(VINTAGE_URLS.keys()), help="Single vintage to download")  
    group.add_argument("--all", action="store_true", help="Download all vintages")
    parser.add_argument("--keep-zip", action="store_true", help="Keep zip file after extraction")
    args = parser.parse_args()                                                             
   
    vintages = list(VINTAGE_URLS.keys()) if args.all else [args.vintage]                   
    for vintage in vintages:
        download_vintage(vintage, keep_zip=args.keep_zip)
                                                                                             
if __name__ == "__main__":                                                                 
    main()