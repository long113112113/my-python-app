import os
from dotenv import load_dotenv

load_dotenv()

VAULT_ADDR = os.environ.get("VAULT_ADDR", "http://192.168.132.135:8200")
VAULT_TOKEN = os.environ.get("VAULT_TOKEN")
VAULT_DB_ROLE = "my-admin-role"
VAULT_CREDS_PATH = f"database/creds/{VAULT_DB_ROLE}"

MYSQL_HOST = os.environ.get("MYSQL_HOST", "192.168.132.135")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))

MYSQL_INITIAL_DB = None


DEFAULT_LEASE_DURATION_WARNING_SECONDS = 30 