from enum import Enum

class Status(Enum):
    MISSING = "MISSING"
    PAID = "PAID"
    REJECTED = "REJECTED"
    ROLLEDBACK = "ROLLEDBACK"

    @staticmethod
    def convertResponseFromDB(status: str) -> Enum:
        match status:
            case "MISSING":
                return Status.MISSING
            case "PAID":
                return Status.PAID
            case "REJECTED":
                return Status.REJECTED
            case "ROLLEDBACK":
                return Status.ROLLEDBACK
            case _:
                raise Exception(f"Unknown order status: {status}")