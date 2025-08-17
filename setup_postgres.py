import os
import subprocess
import sys

def check_postgres_installation():
    """Check if PostgreSQL is installed and running"""
    print("Checking PostgreSQL installation...")
    
    # Check if psql is available
    try:
        result = subprocess.run(['psql', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"PostgreSQL found: {result.stdout.strip()}")
            return True
    except FileNotFoundError:
        print("PostgreSQL not found in PATH")
    
    return False

def install_postgres_instructions():
    """Provide instructions for installing PostgreSQL"""
    print("\n" + "="*60)
    print("POSTGRESQL SETUP INSTRUCTIONS")
    print("="*60)
    print("\n1. Download PostgreSQL for Windows:")
    print("   Visit: https://www.postgresql.org/download/windows/")
    print("   Or use: https://www.enterprisedb.com/downloads/postgres-postgresql-downloads")
    print("\n2. Install PostgreSQL:")
    print("   - Run the installer as Administrator")
    print("   - Choose default port 5432")
    print("   - Set a password for 'postgres' user")
    print("   - Keep default installation directory")
    print("\n3. After installation:")
    print("   - PostgreSQL service should start automatically")
    print("   - You can verify by opening Services (services.msc)")
    print("   - Look for 'postgresql-x64-15' service (or similar)")
    print("\n4. Create the database:")
    print("   - Open pgAdmin (comes with PostgreSQL)")
    print("   - Connect to server")
    print("   - Right-click on 'Databases'")
    print("   - Create new database named 'cnpj'")
    print("\n5. Update the connection string in main.py:")
    print("   DB_CONN = \"dbname=cnpj user=postgres password=YOUR_PASSWORD host=localhost port=5432\"")
    print("\n6. Test connection:")
    print("   python test_db.py")

def check_services():
    """Check if PostgreSQL service is running"""
    print("\nChecking PostgreSQL service...")
    try:
        result = subprocess.run(['sc', 'query', 'postgresql'], capture_output=True, text=True)
        if result.returncode == 0:
            if "RUNNING" in result.stdout:
                print("PostgreSQL service is running")
                return True
            else:
                print("PostgreSQL service is not running")
                return False
    except:
        print("Could not check service status")
    
    return False

def main():
    print("CNPJ Database Setup - PostgreSQL Check")
    print("="*40)
    
    # Check if PostgreSQL is installed
    if not check_postgres_installation():
        install_postgres_instructions()
        return
    
    # Check if service is running
    if not check_services():
        print("\nTo start PostgreSQL service:")
        print("1. Open Services (services.msc)")
        print("2. Find 'postgresql-x64-15' (or similar)")
        print("3. Right-click and select 'Start'")
        print("4. Set startup type to 'Automatic'")
        return
    
    # Test connection
    print("\nTesting database connection...")
    try:
        import psycopg2
        DB_CONN = "dbname=cnpj user=postgres password=yourpass host=localhost port=5432"
        conn = psycopg2.connect(DB_CONN)
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"Connected successfully! PostgreSQL version: {version[0]}")
        cur.close()
        conn.close()
        print("\nPostgreSQL is ready! You can now run:")
        print("python main.py")
    except Exception as e:
        print(f"Connection failed: {e}")
        print("\nPlease:")
        print("1. Create database 'cnpj'")
        print("2. Update password in main.py")
        print("3. Run: python test_db.py")

if __name__ == "__main__":
    main() 