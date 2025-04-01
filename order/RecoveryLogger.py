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

    def get_unfinished_orders(self) -> list[str]:
        try:
            with open(self.file_path, "r") as file:
                incomplete_orders = set()
                for line in file:
                    try:
                        data = json.loads(line.strip())
                        for order_id, status in data.items():
                            if status == "STARTED":
                                incomplete_orders.add(order_id)
                            elif status == "COMPLETED":
                                incomplete_orders.discard(order_id)
                    except json.JSONDecodeError as e:
                        raise e
                return list(incomplete_orders)
        except Exception as e:
            print(e)
            raise e

    def cleanup(self):
        try:
            with open(self.file_path, "w") as file:
                file.write(json.dumps({}))
                file.flush()
        except Exception as e:
            print(e)