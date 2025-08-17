import os
import requests
import zipfile
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings for testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==============================
# CONFIG
# ==============================
BASE_URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
DOWNLOAD_DIR = "downloads"
EXTRACT_DIR = "extracted"

# ==============================
# STEP 1 - Discover all ZIPs
# ==============================
def list_all_files():
    print("Fetching file list from Receita Federal...")
    try:
        # Try with SSL verification disabled
        r = requests.get(BASE_URL, timeout=60, verify=False)
        r.raise_for_status()
    except requests.exceptions.SSLError:
        print("SSL error, trying alternative approach...")
        # Try with different SSL settings
        session = requests.Session()
        session.verify = False
        r = session.get(BASE_URL, timeout=60)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Network error: {e}")
        print("Please check your internet connection and try again.")
        return []
    
    soup = BeautifulSoup(r.text, "html.parser")
    links = [a.get("href") for a in soup.find_all("a", href=True)]
    zips = [l for l in links if l.endswith(".zip")]
    print(f"Found {len(zips)} .zip files")
    return zips

# ==============================
# STEP 2 - Download
# ==============================
def download_files(zips):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    for f in zips:
        url = f"{BASE_URL}{f}"
        path = os.path.join(DOWNLOAD_DIR, f)
        if os.path.exists(path):
            print(f"  Already downloaded: {f}")
            continue
        print(f"Downloading {f}...")
        try:
            with requests.get(url, stream=True, timeout=300, verify=False) as r:
                r.raise_for_status()
                with open(path, "wb") as out:
                    for chunk in r.iter_content(chunk_size=8192):
                        out.write(chunk)
            print(f"  Done: {f}")
        except Exception as e:
            print(f"  Error downloading {f}: {e}")
            continue

# ==============================
# STEP 3 - Extract
# ==============================
def extract_files():
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    for f in os.listdir(DOWNLOAD_DIR):
        if f.endswith(".zip"):
            path = os.path.join(DOWNLOAD_DIR, f)
            try:
                with zipfile.ZipFile(path, "r") as z:
                    print(f"Extracting {f}...")
                    z.extractall(EXTRACT_DIR)
            except Exception as e:
                print(f"Error extracting {f}: {e}")
                continue
    print("All extracted!")

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print("CNPJ Data Download Tool")
    print("="*30)
    print("This will download and extract CNPJ data files.")
    print("Note: Files are large and download may take a long time.")
    print()
    
    # Show available files first
    zips = list_all_files()
    
    if not zips:
        print("No files found. Please check your internet connection.")
        exit(1)
    
    print(f"\nAvailable files:")
    for i, zip_file in enumerate(zips, 1):
        print(f"  {i}. {zip_file}")
    
    print(f"\nTotal size will be several GB. Continue? (y/n): ", end="")
    response = input().lower().strip()
    
    if response in ['y', 'yes']:
        download_files(zips)                # step 1
        extract_files()                     # step 2
        print("\nDownload and extraction complete!")
        print("Files are ready in the 'extracted' folder.")
        print("You can now install PostgreSQL and run main.py to load into database.")
    else:
        print("Download cancelled.") 