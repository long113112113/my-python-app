# vault_client.py
import hvac
import logging
from config import VAULT_ADDR # Giả sử các config này đúng
from config import VAULT_TOKEN, VAULT_DB_ROLE
from hvac.exceptions import VaultError # Import thêm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VaultClient:

    def __init__(self, vault_addr: str, vault_token: str):
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self.client = None
        try:
            self._connect()
        except ConnectionError:
             # Ngăn không cho lỗi dừng chương trình ngay lập tức nếu muốn xử lý ở main
             logging.error("Khoi tao VaultClient that bai do loi ket noi/xac thuc.")
             # self.client sẽ vẫn là None

    def _connect(self):
        if not self.vault_token:
            logging.error("Thieu VAULT_TOKEN.")
            # Nên raise lỗi cụ thể hơn hoặc xử lý khác thay vì chỉ log
            raise ValueError("VAULT_TOKEN không được cung cấp.")
        try:
            logging.debug(f"Dang tao ket noi HVAC den: {self.vault_addr}")
            self.client = hvac.Client(url=self.vault_addr, token=self.vault_token)
            # Kiểm tra xác thực ngay lập tức
            if self.client.is_authenticated():
                logging.info(f"Ket noi va xac thuc Vault thanh cong.")
            else:
                # Token có thể đúng cú pháp nhưng không hợp lệ
                logging.error("Ket noi Vault thanh cong nhung KHONG xac thuc duoc. Kiem tra lai token.")
                # Đặt client về None để is_authenticated() trả về False
                self.client = None
                raise ConnectionError("Token Vault không hợp lệ hoặc không có quyền.")

        except VaultError as ve:
             logging.error(f"Loi Vault khi ket noi/xac thuc tai {self.vault_addr}: {ve}")
             self.client = None # Đảm bảo client là None khi có lỗi
             raise ConnectionError(f"Loi Vault khi ket noi: {ve}")
        except Exception as e:
            # Các lỗi khác (mạng, URL sai định dạng...)
            logging.error(f"Loi khong mong doi khi ket noi Vault tai {self.vault_addr}: {e}")
            self.client = None # Đảm bảo client là None khi có lỗi
            raise ConnectionError(f"Loi ket noi khong xac dinh: {e}")

    # ====> PHƯƠNG THỨC CẦN THÊM <====
    def is_authenticated(self) -> bool:
        """
        Kiểm tra xem client Vault đã được kết nối và xác thực thành công chưa.
        """
        # Chỉ cần kiểm tra self.client có tồn tại và đã xác thực thành công hay không
        # vì _connect đã xử lý việc đặt self.client=None nếu xác thực thất bại.
        return self.client is not None and self.client.is_authenticated()
    # =================================

    def get_db_credentials(self, role_name: str) -> dict | None:
        # Sử dụng phương thức is_authenticated() của chính lớp này
        if not self.is_authenticated():
            logging.error("Client chưa được xác thực, không thể lấy credentials.")
            return None

        logging.info(f"Dang yeu cau credentials cho role: {role_name}")
        try:
            # Sử dụng generate_credentials là đúng cho database secrets engine
            read_response = self.client.secrets.database.generate_credentials(name=role_name)

            # Kiểm tra cấu trúc response trước khi truy cập
            if 'data' not in read_response or not read_response['data']:
                 logging.error(f"Response tu Vault khong co du lieu credentials cho role '{role_name}'.")
                 return None
            if 'lease_id' not in read_response or 'lease_duration' not in read_response:
                 logging.error(f"Response tu Vault thieu lease_id hoac lease_duration cho role '{role_name}'.")
                 return None # Hoặc xử lý khác nếu chấp nhận creds không có lease

            creds = read_response['data']
            lease_id = read_response['lease_id']
            lease_duration = read_response['lease_duration']

            # Kiểm tra các key cần thiết trong creds
            if 'username' not in creds or 'password' not in creds:
                 logging.error(f"Du lieu credentials ('data') tu Vault thieu username hoac password.")
                 return None

            logging.info(f"-> Lay thanh cong User: {creds['username']}, Lease ID: {lease_id[:8]}..., Duration: {lease_duration}s")
            return {
                "username": creds['username'],
                "password": creds['password'],
                "lease_id": lease_id,
                "lease_duration": lease_duration
            }
        except VaultError as ve:
             logging.error(f"Loi Vault khi lay credentials cho role '{role_name}': {ve}")
             return None
        except Exception as e:
            # Bắt lỗi chung cuối cùng
            logging.error(f"Loi khong mong doi khi lay credentials cho role '{role_name}': {e}", exc_info=True)
            return None

    def revoke_lease(self, lease_id: str):
        # Không cần kiểm tra is_authenticated() ở đây nữa nếu muốn thử revoke ngay cả khi client có vấn đề
        # Tuy nhiên, kiểm tra self.client là cần thiết
        if not self.client:
             logging.warning("Khong co client Vault hop le de revoke lease.")
             return
        if not lease_id:
            logging.warning("Khong co lease ID duoc cung cap de revoke.")
            return

        logging.info(f"Dang gui yeu cau revoke cho lease: {lease_id[:8]}...")
        try:
            self.client.sys.revoke_lease(lease_id)
            logging.info(f"-> Yeu cau revoke cho lease {lease_id[:8]}... da duoc gui.")
        except VaultError as ve:
             # Lỗi thường gặp: lease không tồn tại, đã revoke, không có quyền
             logging.warning(f"Loi Vault khi revoke lease {lease_id[:8]}... (co the da het han/bi revoke): {ve}")
        except Exception as e:
            logging.error(f"Loi khong mong doi khi revoke lease {lease_id[:8]}...: {e}")