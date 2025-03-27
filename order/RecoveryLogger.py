import json


class RecoveryLogger:
    def __init__(self, output_file_path):
        self.file_path = output_file_path

    def __load_data(self):
        try:
            with open(self.file_path, "r") as file:
                return json.load(file)
        except Exception as e:
            print(e)
            return {}

    def write_to_log(self, order_id: str, status: str, status_code: int):
        log = self.__load_data()
        with open(self.file_path, 'w') as file:
            log[order_id] = {"status": status, "status_code": status_code}
            file.write(json.dumps(log, indent=2))
            file.flush()

    def find_order(self, order_id):
        try:
            with open(self.file_path, "r") as file:
                return json.load(file)[order_id]
        except Exception as e:
            print(e)
            return None