import logging
import time
from db_manager import DatabaseManager
from mysql.connector import Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def start_interactive_session(db_manager: DatabaseManager):
    """Bắt đầu một phiên SQL tương tác với người dùng."""
    if not db_manager.is_connected():
        logging.error("Không thể bắt đầu phiên: Chưa kết nối database.")
        return

    print("\nĐã kết nối đến database. Nhập lệnh SQL hoặc 'exit'/'quit' để thoát.")
    while True:
        if not db_manager.is_connected():
            print("\nMất kết nối đến database. Phiên kết thúc.")
            break

        try:
            sql_command = input(f"SQL ({db_manager.dynamic_user})> ").strip()

            if not sql_command:
                continue
            if sql_command.lower() in ['quit', 'exit']:
                print("Đang thoát phiên tương tác...")
                break
            start_time = time.time()
            cursor = db_manager.execute_sql(sql_command) 
            end_time = time.time()

            if cursor:
                logging.info(f"Lệnh thực thi trong: {end_time - start_time:.4f} giây")
                is_dml = False 
                try:
                    if cursor.description:
                        columns = [col[0] for col in cursor.description]
                        rows = cursor.fetchall()
                        if rows:
                            print(" | ".join(columns))
                            print("-" * (len(" | ".join(columns))))
                            for row in rows:
                                print(" | ".join(map(str, row)))
                        else:
                            print(f"Thành công: Lệnh trả về 0 dòng.")
                    else:
                        is_dml = True
                        print(f"Thành công: Số dòng bị ảnh hưởng: {cursor.rowcount}")
                        db_manager.commit()
                        print("Đã commit thay đổi.")

                except Error as e:
                    logging.error(f"Lỗi khi xử lý kết quả: {e}")
                    print("Đang rollback do lỗi xử lý kết quả...")
                    db_manager.rollback()
                finally:
                    if cursor:
                        try:
                            cursor.close()
                        except Error as ce:
                             logging.warning(f"Loi khi dong cursor: {ce}")
            else:
                print("Lỗi khi thực thi lệnh trên database. Kiểm tra log để biết chi tiết.")
        except (EOFError, KeyboardInterrupt):
            print("\nNhận tín hiệu thoát (EOF/Ctrl+C). Đang thoát phiên...")
            break
        except Exception as e:
            logging.error(f"Lỗi không mong muốn trong phiên tương tác: {e}", exc_info=True)
            if db_manager.is_connected():
                 print("Đang rollback do lỗi không mong muốn...")
                 db_manager.rollback()
            else:
                 print("Mất kết nối đến database sau lỗi.")
                 break 

    logging.info("Kết thúc phiên SQL tương tác.")