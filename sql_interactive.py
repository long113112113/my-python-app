import logging
import time
from db_manager import DatabaseManager
from mysql.connector import Error 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def start_interactive_session(db_manager: DatabaseManager):
    if not db_manager.is_connected():
        logging.error("chua ket noi database")
        return

    print("\nda ket noi den database (nhap 'exit' hoac 'quit' de thaot)")
    while True:
        if not db_manager.is_connected():
            print("khong con ket noi den database")
            break
        try:
            sql_command = input(f"SQL ({db_manager.dynamic_user})> ").strip()

            if not sql_command:
                continue
            if sql_command.lower() in ['quit', 'exit']:
                print("dang thoat...")
                break
            
            if not db_manager.is_connected():
                print("khong con ket noi den database")
                break

            start_time = time.time()
            cursor = db_manager.execute_sql(sql_command)
            end_time = time.time()

            if cursor:
                logging.info(f"hoan thanh: {end_time - start_time:.4f} s")
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
                             print(f"Execute thanh cong nhung khong co dong nao: {cursor.rowcount}")
                    else:
                         print(f"Execute loi tai dong: {cursor.rowcount}")
                         db_manager.commit()

                except Error as e:
                    logging.error(f"loi khi xuat ket qua: {e}")
                    db_manager.rollback()
                finally:
                     if cursor:
                         cursor.close()
            else:
                 print("loi khi execute lenh tren database")
            if db_manager.is_connected():
                db_manager.rollback()
            else:
                print("khong con ket noi den database")
                break
        except (EOFError, KeyboardInterrupt):
            print("\ndang thoat...")
            break
        except Exception as e:
            logging.error(f"loi khi dang su dung mysql: {e}")
            if db_manager.is_connected():
                db_manager.rollback()
            else:
                print("khong con ket noi den database")
                break
            
