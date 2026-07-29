from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._total_processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("Storage is empty.")

        rank = self._total_processed - len(self._storage)
        data = self._storage.pop(0)

        return rank, data


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, int | float) and not isinstance(data, bool):
            return True
        if isinstance(data, list) and data:
            return all(isinstance(x, (int, float))
                       and not isinstance(x, bool) for x in data)

        return False

    def ingest(self, data: int | float | list[float | int]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data.")

        if isinstance(data, int | float):
            self._storage.append(str(data))
            self._total_processed += 1
        else:
            for i in data:
                self._storage.append(str(i))
                self._total_processed += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str) and data:
            return True

        if isinstance(data, list) and data:
            return all(isinstance(x, str) for x in data)

        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data.")

        if isinstance(data, str):
            self._storage.append(data)
            self._total_processed += 1

        else:
            for i in data:
                self._storage.append(i)
                self._total_processed += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:

        def is_valid_dict(data: dict) -> bool:
            return all(isinstance(k, str) and
                       isinstance(v, str) for k, v in data.items())

        if isinstance(data, dict) and data:
            if is_valid_dict(data):
                return True

        if isinstance(data, list) and data:

            return all(is_valid_dict(x) for x in data)

        return False

    def ingest(self, data: dict[str, str] |
               list[dict[str, str]]) -> None:

        if not self.validate(data):
            raise ValueError("Improper Log data")

        if isinstance(data, dict):
            log_data = (
                f"{data.get('log_level', '')}:"
                f" {data.get('log_message', '')}"
            )
            self._storage.append(log_data)
            self._total_processed += 1

        else:
            for item in data:
                log_data = (
                    f"{item.get('log_level', '')}: "
                    f"{item.get('log_message', '')}"
                )
                self._storage.append(log_data)
                self._total_processed += 1


def test_num_processor() -> None:
    print("Testing Numeric processor...")
    num_proc = NumericProcessor()

    inputs: list[Any] = [42, "Hello"]
    for input in inputs:
        print(f" Trying to validate input {input}:"
              f" {num_proc.validate(input)}")

    print(" Test invalid ingestion of string 'foo' without "
          "prior validation:")
    try:
        num_proc.ingest("foo")  # type: ignore
    except ValueError as e:
        print(f" Got exception: {e}")

    data: list = [1, 2, 3, 4, 5]
    print(f" Processing data: {data}")
    num_proc.ingest(data)

    print(" Extracting 3 values...")
    for i in range(3):
        rank, value = num_proc.output()
        print(f" Numeric value {rank}: {value}")


def test_text_processor() -> None:
    print("Testing Text Processor...")
    text_proc = TextProcessor()

    print(f" Trying to validate input '42': {text_proc.validate('42')}")

    data: list[Any] = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {data}")
    text_proc.ingest(data)

    print(" Extracting 1 value...")
    rank, value = text_proc.output()
    print(f" Text value {rank}: {value}")


def test_log_processor() -> None:
    print("Testing Log Processor...")
    log_proc = LogProcessor()

    print(f" Trying to validate input "
          f"'Hello': {log_proc.validate('Hello')}")

    data: list[dict[str, str]] = [{
        'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]

    print(f" Processing data: {data}")
    log_proc.ingest(data)

    print(" Extracting 2 values...")
    for i in range(2):
        rank, value = log_proc.output()
        print(f" Log entry {rank}: {value}")


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")
    test_num_processor()
    print()
    test_text_processor()
    print()
    test_log_processor()


if __name__ == "__main__":
    main()
