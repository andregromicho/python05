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


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    break
            else:
                print(f"DataStream error - Can't "
                      f"process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if not self._processors:
            print("No processor found, no data")

        for proc in self._processors:
            name: str = proc.__class__.__name__
            proc_name: str = name.replace("Processor", " Processor")
            processed: int = proc._total_processed
            stored: int = len(proc._storage)
            print(f"{proc_name}: total {processed} "
                  f"items processed, remaining {stored} on processor")
        print()


def get_data() -> list:
    data: list[Any] = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO',
         'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]
    return data


def test_data_stream_1(stream: DataStream, proc: NumericProcessor) -> None:
    stream.print_processors_stats()

    print("Registering Numeric Processor\n")
    stream.register_processor(proc)

    batch = get_data()
    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()


def test_data_stream_2(stream: DataStream, text_proc: TextProcessor,
                       log_proc: LogProcessor) -> None:
    print("Registering other data processors")

    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    print("Send the same batch again")
    batch = get_data()
    stream.process_stream(batch)
    stream.print_processors_stats()


def test_data_stream_3(stream: DataStream, text_proc: TextProcessor,
                       log_proc: LogProcessor,
                       num_proc: NumericProcessor) -> None:
    print("Consuming some elements from the data processors:"
          " Numeric 3, Text 2, Log 1")

    for _ in range(3):
        try:
            num_proc.output()
        except IndexError as e:
            print(str(e))
            continue
    for _ in range(2):
        try:
            text_proc.output()
        except IndexError as e:
            print(str(e))
            continue
    try:
        log_proc.output()
    except IndexError as e:
        print(str(e))

    stream.print_processors_stats()


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")

    print("Initialize Data Stream...")
    stream = DataStream()
    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()

    test_data_stream_1(stream, num_proc)
    test_data_stream_2(stream, text_proc, log_proc)
    test_data_stream_3(stream, text_proc, log_proc, num_proc)


if __name__ == "__main__":
    main()
