import os
import re
import requests
import zipfile
import psycopg2
import pandas as pd
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

DB_CONN = "dbname=cnpj user=postgres password=yourpass host=localhost port=5432"

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
            with zipfile.ZipFile(path, "r") as z:
                print(f"Extracting {f}...")
                z.extractall(EXTRACT_DIR)
    print("All extracted!")

# ==============================
# STEP 4 - Schema
# ==============================
def create_schema():
    schema_sql = """
    DROP TABLE IF EXISTS empresas CASCADE;
    DROP TABLE IF EXISTS estabelecimentos CASCADE;
    DROP TABLE IF EXISTS socios CASCADE;

    CREATE TABLE empresas (
        cnpj_basico CHAR(8) PRIMARY KEY,
        razao_social TEXT,
        natureza_juridica CHAR(4),
        qualificacao_responsavel CHAR(2),
        capital_social NUMERIC,
        porte CHAR(2),
        ente_federativo CHAR(1)
    );

    CREATE TABLE estabelecimentos (
        cnpj_basico CHAR(8),
        cnpj_ordem CHAR(4),
        cnpj_dv CHAR(2),
        matriz_filial CHAR(1),
        nome_fantasia TEXT,
        situacao_cadastral CHAR(2),
        data_situacao DATE,
        motivo_situacao CHAR(2),
        nm_cidade_exterior TEXT,
        pais CHAR(3),
        data_inicio DATE,
        cnae_principal CHAR(7),
        cnaes_secundarios TEXT,
        tipo_logradouro TEXT,
        logradouro TEXT,
        numero TEXT,
        complemento TEXT,
        bairro TEXT,
        cep CHAR(8),
        uf CHAR(2),
        municipio CHAR(4),
        telefone1 CHAR(12),
        telefone2 CHAR(12),
        email TEXT,
        PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
    );

    CREATE TABLE socios (
        cnpj_basico CHAR(8),
        tipo_socio CHAR(1),
        nome TEXT,
        cnpj_cpf_socio CHAR(14),
        qualificacao_socio CHAR(2),
        data_entrada DATE,
        pais CHAR(3),
        representante_nome TEXT,
        representante_cpf CHAR(11),
        representante_qualificacao CHAR(2)
    );
    """
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    cur.execute(schema_sql)
    conn.commit()
    cur.close()
    conn.close()
    print("Schema created!")

# ==============================
# STEP 5 - Load Data
# ==============================
def load_data():
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    mappings = {
        "Empresas": ("empresas", ["cnpj_basico","razao_social","natureza_juridica","qualificacao_responsavel","capital_social","porte","ente_federativo"]),
        "Estabelecimentos": ("estabelecimentos", ["cnpj_basico","cnpj_ordem","cnpj_dv","matriz_filial","nome_fantasia","situacao_cadastral","data_situacao","motivo_situacao","nm_cidade_exterior","pais","data_inicio","cnae_principal","cnaes_secundarios","tipo_logradouro","logradouro","numero","complemento","bairro","cep","uf","municipio","telefone1","telefone2","email"]),
        "Socios": ("socios", ["cnpj_basico","tipo_socio","nome","cnpj_cpf_socio","qualificacao_socio","data_entrada","pais","representante_nome","representante_cpf","representante_qualificacao"]),
    }

    for file in os.listdir(EXTRACT_DIR):
        for prefix, (table, cols) in mappings.items():
            if file.startswith(prefix) and file.endswith(".txt"):
                filepath = os.path.join(EXTRACT_DIR, file)
                print(f"Loading {filepath} -> {table}")
                with open(filepath, "r", encoding="latin1") as f:
                    cur.copy_from(f, table, sep="|", null="", columns=cols)
                conn.commit()
                print(f"  Loaded {file}")

    cur.close()
    conn.close()

# ==============================
# STEP 6 - Build Clean Table
# ==============================
def build_clean_table():
    sql = """
    DROP TABLE IF EXISTS empresas_publico;
    CREATE TABLE empresas_publico AS
    SELECT
        e.razao_social AS nome_empresa,
        e.cnpj_basico || est.cnpj_ordem || est.cnpj_dv AS cnpj,
        e.natureza_juridica,
        est.situacao_cadastral,
        est.data_situacao,
        e.porte,
        CASE WHEN e.porte = 'ME' THEN 'Sim' ELSE 'Nao' END AS mei,
        COALESCE(est.telefone1, est.telefone2) AS telefone,
        est.email
    FROM empresas e
    JOIN estabelecimentos est
        ON e.cnpj_basico = est.cnpj_basico
    WHERE est.matriz_filial = '1';
    """
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    print("Clean table ready!")

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    zips = list_all_files()             # step 1
    download_files(zips)                # step 2
    extract_files()                     # step 3
    create_schema()                     # step 4
    load_data()                         # step 5
    build_clean_table()                 # step 6
    print("All data ready in PostgreSQL!")
