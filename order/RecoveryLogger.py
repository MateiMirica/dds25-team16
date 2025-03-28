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
                file.write(json.dumps({}))
                file.flush()

    def __load_data(self):
        try:
            with open(self.file_path, "r") as file:
                return json.load(file)
        except Exception as e:
            print(e)
            return {}

    def write_to_log(self, order_id: str, status: str):
        log = self.__load_data()
        with open(self.file_path, 'w') as file:
            log[order_id] = status
            file.write(json.dumps(log, indent=2))
            file.flush()

    def find_order(self, order_id):
        try:
            with open(self.file_path, "r") as file:
                return json.load(file)[order_id]
        except Exception as e:
            print(e)
            return None

    def get_unfinished_orders(self):
        try:
            with open(self.file_path, "r") as file:
                data = json.load(file)
                unfinished_orders = [
                    order_id for order_id, status in data.items() if status == "STARTED"
                ]
        except Exception as e:
            print(e)
            return []
