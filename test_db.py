import psycopg2

# Test database connection
DB_CONN = "dbname=cnpj user=postgres password=yourpass host=localhost port=5432"

try:
    print("Testing database connection...")
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print(f"Connected successfully! PostgreSQL version: {version[0]}")
    cur.close()
    conn.close()
    print("Database connection test passed!")
except Exception as e:
    print(f"Database connection failed: {e}")
    print("\nPlease make sure:")
    print("1. PostgreSQL is installed and running")
    print("2. Database 'cnpj' exists")
    print("3. Update DB_CONN in main.py with correct credentials") 