# CNPJ Database Setup Guide

This guide will help you set up the CNPJ database project step by step.

## Quick Start

1. **Install Python dependencies**: `pip install -r requirements.txt`
2. **Install PostgreSQL**: Follow the instructions below
3. **Test download**: `python download_only.py`
4. **Test database**: `python test_db.py`
5. **Run full process**: `python main.py`

## Step 1: Install PostgreSQL

### Option A: Download from Official Site
1. Visit: https://www.postgresql.org/download/windows/
2. Download the latest version for Windows
3. Run installer as Administrator
4. Set password for 'postgres' user
5. Keep default port 5432

### Option B: Download from EnterpriseDB
1. Visit: https://www.enterprisedb.com/downloads/postgres-postgresql-downloads
2. Download PostgreSQL for Windows
3. Follow installation wizard

### After Installation
1. PostgreSQL service should start automatically
2. Verify by opening Services (services.msc)
3. Look for 'postgresql-x64-15' service (or similar)

## Step 2: Create Database

1. Open pgAdmin (comes with PostgreSQL)
2. Connect to server using your password
3. Right-click on 'Databases'
4. Create new database named 'cnpj'

## Step 3: Configure Connection

Edit `main.py` and update the database connection:

```python
DB_CONN = "dbname=cnpj user=postgres password=YOUR_PASSWORD host=localhost port=5432"
```

Replace `YOUR_PASSWORD` with the password you set during PostgreSQL installation.

## Step 4: Test Setup

### Test Database Connection
```bash
python test_db.py
```

### Test Download (Optional)
```bash
python download_only.py
```

## Step 5: Run Full Process

```bash
python main.py
```

This will:
1. Download all CNPJ data files (~10GB+)
2. Extract the files
3. Create database tables
4. Load all data into PostgreSQL
5. Create a clean, processed table

## Troubleshooting

### PostgreSQL Connection Issues
- Make sure PostgreSQL service is running
- Check if port 5432 is not blocked by firewall
- Verify database 'cnpj' exists
- Confirm password in connection string

### Download Issues
- Check internet connection
- Ensure enough disk space (at least 50GB free)
- Files are large and download may take hours

### Memory Issues
- The script processes large files
- Consider increasing PostgreSQL shared_buffers
- Monitor system memory usage

## File Structure

```
database/
├── main.py              # Main script (download + database)
├── download_only.py     # Download only (no database required)
├── test_db.py          # Test database connection
├── setup_postgres.py   # PostgreSQL setup helper
├── requirements.txt    # Python dependencies
├── README.md          # Project documentation
├── SETUP.md           # This setup guide
├── downloads/         # Downloaded ZIP files (created automatically)
└── extracted/         # Extracted data files (created automatically)
```

## Database Tables

After running the script, you'll have these tables:

- **empresas**: Basic company information
- **estabelecimentos**: Company establishments/branches  
- **socios**: Company partners/shareholders
- **empresas_publico**: Clean, processed data for queries

## Sample Queries

```sql
-- Count total companies
SELECT COUNT(*) FROM empresas_publico;

-- Find companies by state
SELECT COUNT(*) FROM empresas_publico WHERE uf = 'SP';

-- Find MEI companies
SELECT COUNT(*) FROM empresas_publico WHERE mei = 'Sim';

-- Search by company name
SELECT * FROM empresas_publico WHERE nome_empresa ILIKE '%microsoft%';
```

## Performance Tips

- The initial data load may take several hours
- Consider creating indexes on frequently queried columns
- Monitor disk space during processing
- The final database will be several GB in size 