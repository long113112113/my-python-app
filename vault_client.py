import hvac
import logging
from config import VAULT_ADDR
from config import VAULT_TOKEN, VAULT_DB_ROLE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VaultClient:

    def __init__(self, vault_addr: str, vault_token: str):
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self.client = None
        self._connect()

    def _connect(self):
        if not self.vault_token:
            logging.error("thieu token")
            raise ValueError("VAULT_TOKEN = ?.")
        try:
            self.client = hvac.Client(url=self.vault_addr, token=self.vault_token)
            if not self.client.is_authenticated():
                logging.error("sai token")
                raise ConnectionError("xac thuc that bai")
            logging.info("xac thuc thanh cong")
        except Exception as e:
            logging.error(f"loi khi ket noi: {self.vault_addr}: {e}")
            raise ConnectionError(f"ket noi that bai {e}")

    def get_db_credentials(self, role_name: str) -> dict | None:
        if not self.client or not self.client.is_authenticated():
            logging.error("chua xac thuc client")
            return None

        logging.info(f"dang yeu cau cre tai path: {role_name}")
        try:
            read_response = self.client.secrets.database.generate_credentials(name=role_name)
            creds = read_response['data']
            lease_id = read_response['lease_id']
            lease_duration = read_response['lease_duration']

            logging.info(f"-> lay thanh cong User: {creds['username']}, Lease ID: {lease_id}, Duration: {lease_duration}s")
            return {
                "username": creds['username'],
                "password": creds['password'],
                "lease_id": lease_id,
                "lease_duration": lease_duration
            }
        except Exception as e:
            logging.error(f"khong the lay cre : '{role_name}': {e}", exc_info=True)
            return None
    #revoke stuff
    def revoke_lease(self, lease_id: str):
        if not self.client or not self.client.is_authenticated() or not lease_id:
            logging.warning("khong the revoke client chua xuc thuc")
            return

        logging.info(f"dang revoke lease: {lease_id}")
        try:
            self.client.sys.revoke_lease(lease_id)
            logging.info(f"-> da revoke {lease_id}.")
        except Exception as e:
            logging.error(f"loi khi revoke{lease_id}: {e}")

# if __name__ == '__main__':
#     try:
#         v_client = VaultClient(vault_addr=VAULT_ADDR, vault_token=VAULT_TOKEN)
#         db_creds = v_client.get_db_credentials(VAULT_DB_ROLE)
#         if db_creds:
#             print("credentials:")
#             print(db_creds)
#             # test revoke lease
#             v_client.revoke_lease(db_creds['lease_id'])
#     except (ValueError, ConnectionError) as e:
#         print(f"error: {e}")
