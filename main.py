import logging
import threading
import time
from config import (
    VAULT_ADDR, VAULT_TOKEN, VAULT_DB_ROLE,
    MYSQL_HOST, MYSQL_PORT, MYSQL_INITIAL_DB,
    DEFAULT_LEASE_DURATION_WARNING_SECONDS
)
from vault_client import VaultClient
from db_manager import DatabaseManager
from sql_interactive import start_interactive_session
import vault_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def lease_expiry_monitor(vault_client: VaultClient,db_manager: DatabaseManager, lease_duration: int, lease_id: str):
    
    buffer_seconds = 10
    sleep_duration = lease_duration - buffer_seconds
    if sleep_duration <= 0:
        logging.warning("thoi gian lease khong hop le")
        return
    logging.info(f"thoi gian lease: {lease_duration} s")
    time.sleep(sleep_duration)
    logging.info(f"Ket noi : '{lease_id}' da duoc dong")
    if db_manager.is_connected():
        db_manager.close()
    else:
        logging.warning("khong con ket noi den database")
    if vault_client and lease_id:
        try:
            vault_client.revoke_lease(lease_id)
        except Exception as e:
            logging.error(f"loi khi revoke lease: {lease_id}: {e}")
    else:
        logging.warning(f"ko the revoke: {lease_id}")
        
def main():
    vault_client = None
    db_manager = None
    lease_id = None
    expiry_thread = None
    try:
        # init vault
        if not VAULT_TOKEN:
             logging.error("thieu token trong .env")
             return
        vault_client = VaultClient(vault_addr=VAULT_ADDR, vault_token=VAULT_TOKEN)

        
        db_creds = vault_client.get_db_credentials(VAULT_DB_ROLE)
        if not db_creds:
            return

        lease_id = db_creds['lease_id']
        lease_duration = db_creds['lease_duration']

        # connect to mysql
        db_manager = DatabaseManager(host=MYSQL_HOST, port=MYSQL_PORT, initial_db=MYSQL_INITIAL_DB)
        if not db_manager.connect(username=db_creds['username'], password=db_creds['password']):
            return 

        expiry_thread = threading.Thread(
            target=lease_expiry_monitor,
            args=(vault_client,db_manager, lease_duration, lease_id),
            daemon=True
        )
        expiry_thread.start()


        start_interactive_session(db_manager)

    except (ValueError, ConnectionError) as e:
        logging.error(f"loi ket noi: {e}")
    except Exception as e:
        logging.critical(f"loi: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()

        if vault_client and lease_id:
            vault_client.revoke_lease(lease_id)
        elif lease_id:
             logging.warning(f"ko the revoke: {lease_id}")

        logging.info("bye")


if __name__ == "__main__":
    main()