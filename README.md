# CNPJ Database Scraper

This project downloads and processes CNPJ (Brazilian company registration) data from Receita Federal and stores it in a PostgreSQL database.

## Prerequisites

1. **PostgreSQL Database**: You need a PostgreSQL database running
2. **Python 3.8+**: Make sure Python is installed on your system

## Setup

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database Connection

Edit the `DB_CONN` variable in `main.py` with your PostgreSQL connection details:

```python
DB_CONN = "dbname=cnpj user=postgres password=yourpass host=localhost port=5432"
```

Replace:
- `cnpj` with your database name
- `postgres` with your database user
- `yourpass` with your database password
- `localhost` with your database host
- `5432` with your database port

### 3. Create Database (if needed)

If you haven't created the database yet, connect to PostgreSQL and run:

```sql
CREATE DATABASE cnpj;
```

## Usage

Run the script to download and process all CNPJ data:

```bash
python main.py
```

The script will:
1. Download all CNPJ data files from Receita Federal
2. Extract the ZIP files
3. Create database tables
4. Load all data into PostgreSQL
5. Create a clean, processed table called `empresas_publico`

## Database Tables

The script creates the following tables:

- **empresas**: Basic company information
- **estabelecimentos**: Company establishments/branches
- **socios**: Company partners/shareholders
- **empresas_publico**: Clean, processed data for public use

## Output

All data is stored in PostgreSQL. The main table for queries is `empresas_publico` which contains:
- Company name
- Full CNPJ number
- Legal nature
- Registration status
- Registration date
- Company size
- MEI status (micro-entrepreneur)
- Phone number
- Email

## Notes

- The script downloads large files and may take significant time
- Make sure you have enough disk space for downloads and extraction
- The database will require substantial storage space
- Processing may take several hours depending on your system 