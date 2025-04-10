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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
lease_revoked_state = {'revoked': False}
revoke_lock = threading.Lock()

def lease_expiry_monitor(
    vault_client: VaultClient,
    db_manager: DatabaseManager,
    lease_duration: int,
    lease_id: str,
    shared_state: dict,
    lock: threading.Lock 
    ):
    buffer_seconds = DEFAULT_LEASE_DURATION_WARNING_SECONDS if DEFAULT_LEASE_DURATION_WARNING_SECONDS > 0 else 10
    sleep_duration = lease_duration - buffer_seconds

    if sleep_duration <= 0:
        logging.warning(f"[Monitor-{lease_id[:8]}] Thời gian lease ({lease_duration}s) quá ngắn hoặc buffer ({buffer_seconds}s) quá lớn, sẽ đóng và revoke ngay.")
        sleep_duration = 0 
    else:
        logging.info(f"[Monitor-{lease_id[:8]}] Sẽ kiểm tra lease sau khoảng {sleep_duration:.2f} giây (Lease duration: {lease_duration}s)")

    time.sleep(sleep_duration)

    logging.info(f"[Monitor-{lease_id[:8]}] Thời gian chờ kết thúc, chuẩn bị đóng kết nối và kiểm tra revoke.")
    if db_manager and db_manager.is_connected():
        logging.info(f"[Monitor-{lease_id[:8]}] Đang đóng kết nối DB...")
        db_manager.close() 
    else:
        logging.warning(f"[Monitor-{lease_id[:8]}] Không có kết nối DB để đóng hoặc đã đóng.")
    with lock:
        if not shared_state['revoked']:
            if vault_client and lease_id:
                try:
                    logging.info(f"[Monitor-{lease_id[:8]}] Đang thu hồi lease...")
                    vault_client.revoke_lease(lease_id)
                    shared_state['revoked'] = True
                    logging.info(f"[Monitor-{lease_id[:8]}] Thu hồi lease thành công.")
                except Exception as e:
                    logging.error(f"[Monitor-{lease_id[:8]}] Lỗi khi thu hồi lease: {e}")
            else:
                logging.warning(f"[Monitor-{lease_id[:8]}] Không thể thu hồi lease: thiếu vault_client hoặc lease_id.")
        else:
            logging.info(f"[Monitor-{lease_id[:8]}] Lease đã được thu hồi bởi luồng khác, không cần thực hiện lại.")

def main():
    global lease_revoked_state, revoke_lock 

    vault_client_instance = None 
    db_manager = None
    lease_id = None
    expiry_thread = None

    try:
        if not VAULT_TOKEN:
             logging.error("Thiếu VAULT_TOKEN trong cấu hình (.env hoặc biến môi trường).")
             return
        vault_client_instance = VaultClient(vault_addr=VAULT_ADDR, vault_token=VAULT_TOKEN)
        if not vault_client_instance.is_authenticated():
             logging.error("Xác thực Vault thất bại. Kiểm tra địa chỉ và token.")
             return
        logging.info("Xác thực Vault thành công.")

        db_creds = vault_client_instance.get_db_credentials(VAULT_DB_ROLE)
        if not db_creds:
            logging.error("Không thể lấy credentials từ Vault.")
            return

        lease_id = db_creds['lease_id']
        lease_duration = db_creds['lease_duration']
        username = db_creds['username']
        password = db_creds['password']
        logging.info(f"Lấy thành công credentials cho user: {username}, Lease ID: {lease_id[:8]}..., Duration: {lease_duration}s")
        db_manager = DatabaseManager(host=MYSQL_HOST, port=MYSQL_PORT, initial_db=MYSQL_INITIAL_DB)
        if not db_manager.connect(username=username, password=password):
            logging.error("Kết nối tới MySQL thất bại.")
            return
        logging.info("Kết nối MySQL thành công.")

        with revoke_lock:
            lease_revoked_state['revoked'] = False

        expiry_thread = threading.Thread(
            target=lease_expiry_monitor,
            args=(
                vault_client_instance,
                db_manager,
                lease_duration,
                lease_id,
                lease_revoked_state,
                revoke_lock         
            ),
            daemon=True 
        )
        expiry_thread.start()
        logging.info(f"Đã khởi động luồng theo dõi cho lease: {lease_id[:8]}...")

        start_interactive_session(db_manager)
        logging.info("Phiên tương tác SQL kết thúc.")

    except (ValueError, ConnectionError) as e:
        logging.error(f"Lỗi cấu hình hoặc kết nối ban đầu: {e}")
    except KeyboardInterrupt:
        logging.info("Nhận tín hiệu KeyboardInterrupt (Ctrl+C), đang thoát...")
    except Exception as e:
        logging.critical(f"Lỗi không mong muốn trong luồng chính: {e}", exc_info=True) 
    finally:
        logging.info("Bắt đầu quá trình dọn dẹp (finally)...")

        if db_manager and db_manager.is_connected():
            logging.info("[Main-Finally] Đang đóng kết nối DB...")
            db_manager.close()
        else:
            logging.info("[Main-Finally] Kết nối DB đã đóng hoặc chưa được tạo.")

        if lease_id: 
            with revoke_lock:
                if not lease_revoked_state['revoked']:
                    if vault_client_instance:
                        try:
                            logging.info(f"[Main-Finally] Đang thu hồi lease: {lease_id[:8]}...")
                            vault_client_instance.revoke_lease(lease_id)
                            lease_revoked_state['revoked'] = True
                            logging.info(f"[Main-Finally] Thu hồi lease thành công.")
                        except Exception as e:
                            logging.warning(f"[Main-Finally] Lỗi khi thu hồi lease (có thể đã hết hạn/bị revoke bởi monitor): {e}")
                    else:
                        logging.warning(f"[Main-Finally] Không có Vault client để thu hồi lease: {lease_id[:8]}...")
                else:
                    logging.info(f"[Main-Finally] Lease {lease_id[:8]}... đã được thu hồi bởi luồng monitor.")
        else:
            logging.info("[Main-Finally] Không có lease ID để thu hồi.")
        logging.info("All done!")


if __name__ == "__main__":
    main()