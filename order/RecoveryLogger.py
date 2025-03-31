import json
import os

class RecoveryLogger:
    def __init__(self, output_file_path):
        self.file_path = output_file_path
        self.__ensure_file_exists()

    def __ensure_file_exists(self):
        print(self.file_path)
        if not os.path.exists(self.file_path):
            with open(self.file_path, "w") as file:
                file.write("")
                file.flush()

    def write_to_log(self, order_id: str, status: str):
        log_entry = {order_id: status}
        with open(self.file_path, "a") as file:
            file.write(json.dumps(log_entry) + '\n')
            file.flush()

    def find_order(self, order_id):
        try:
            with open(self.file_path, "r") as file:
                return json.load(file)[order_id]
        except Exception as e:
            print(e)
            return None

    def get_unfinished_orders(self) -> list[str]:
        try:
            with open(self.file_path, "r") as file:
                d = set()
                data = json.load(file)
                for order_id, status in data.items():
                    if status == "STARTED":
                        d.add(order_id)
                    elif order_id in d and status == "COMPLETED":
                        d.remove(order_id)
                return list(d)
        except Exception as e:
            print(e)
            return []

    def cleanup(self):
        try:
            with open(self.file_path, "w") as file:
                file.write(json.dumps({}))
                file.flush()
        except Exception as e:
            print(e)