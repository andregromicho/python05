from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self):
        self._storage = []
        self._output_rank = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("No data available")

        value: str = self._storage.pop(0)
        rank: int = self._output_rank
        self._output_rank += 1
        return (rank, value)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
            if isinstance(data, (int, float)) and not isinstance(data, bool):
                return True
            if isinstance(data, list):
                return all(isinstance(item, (int, float))
                           and not isinstance(item, bool) for item in data)
            return False

    def ingest(self, data: int | float | list[int] |
               list[float] | list[int | float]) -> None:
        if not self.validate(data):
            raise TypeError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(str(item))
        else:
            self._storage.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(item)
        else:
            self._storage.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in data.items()
            )

        if isinstance(data, list):
            return all(
                isinstance(log, dict) and
                all(isinstance(key, str) and isinstance(value, str)
                    for key, value in log.items())
                for log in data
            )
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")

        if isinstance(data, list):
            for log in data:
                level = log.get("log_level", "")
                message = log.get("log_message", "")
                self._storage.append(f"{level}: {message}")
        else:
            level = data.get("log_level", "")
            message = data.get("log_message", "")
            self._storage.append(f"{level}: {message}")


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []
        self.processed_counts: dict[DataProcessor, int] = {}

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)
        self.processed_counts[proc] = 0

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            handled = False
            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)

                    if isinstance(item, list):
                        count = len(item)
                    else:
                        count = 1

                    self.processed_counts[proc] += count
                    handled = True
                    break

            if not handled:
                print(
                    "DataStream error - Can't process element"
                    f" in stream: {item}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return

        for proc in self.processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            total = self.processed_counts[proc]
            remaining = len(proc._storage)

            print(
                f"{name}: total {total} items processed,"
                f" remaining {remaining} on processor"
            )


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    print()

    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    print()

    print("Registering Numeric Processor")
    print()
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)

    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {
                'log_level': 'WARNING',
                'log_message': 'Telnet access! Use ssh instead'
            },
            {
                'log_level': 'INFO',
                'log_message': 'User wil is connected'
            }
        ],
        42,
        ['Hi', 'five']
    ]

    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()
    print()

    print("Registering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()
    print()

    print(
        "Consume some elements from the data processors:"
        " Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        text_proc.output()
    for _ in range(1):
        log_proc.output()

    stream.print_processors_stats()
