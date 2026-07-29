from abc import ABC, abstractmethod
from typing import Any, Protocol


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

        def is_valid_dict(d: dict) -> bool:
            return all(isinstance(k, str) and
                       isinstance(v, str) for k, v in d.items())

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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")

        if not data:
            return

        values = [item[1] for item in data]
        values_csv = ",".join(values)
        print(values_csv)


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")

        if not data:
            return

        file_json: list[str] = []

        for k, v in data:
            file_json.append(f'"item_{k}": "{v}"')
        content = ", ".join(file_json)
        file = "{" + content + "}"
        print(file)


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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            data: list[tuple[int, str]] = []

            for _ in range(nb):
                try:
                    rank, text = proc.output()
                    data.append((rank, text))
                except IndexError:
                    break

            if data:
                plugin.process_output(data)


def test_data_stream_1(stream: DataStream, n_proc: NumericProcessor,
                       t_proc: TextProcessor, l_proc: LogProcessor,
                       plugin: ExportPlugin) -> None:
    stream.print_processors_stats()

    print("Registering Processors\n")
    stream.register_processor(n_proc)
    stream.register_processor(t_proc)
    stream.register_processor(l_proc)

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
    print(f"Send first batch of data on stream: {data}\n")
    stream.process_stream(data)
    stream.print_processors_stats()

    print("Send 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, plugin)
    print()

    stream.print_processors_stats()


def test_data_stream_2(stream: DataStream, plugin: ExportPlugin) -> None:
    data: list[Any] = [21,
                       ['I love AI',
                        'LLMs are wonderful',
                        'Stay healthy'],
                       [{'log_level': 'ERROR',
                        'log_message': '500 server crash'},
                        {'log_level': 'NOTICE',
                        'log_message': 'Certificate expires in 10 days'}],
                       [32, 42, 64, 84, 128, 168],
                       'World hello']

    print(f"Send another batch of data: {data}\n")
    stream.process_stream(data)
    stream.print_processors_stats()

    print("Send 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, plugin)
    print()

    stream.print_processors_stats()


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===\n")

    print("Initialize Data Stream...\n")
    stream = DataStream()
    csv_plugin = CSVPlugin()
    json_plugin = JSONPlugin()
    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()

    test_data_stream_1(stream, num_proc, text_proc, log_proc, csv_plugin)
    test_data_stream_2(stream, json_plugin)


if __name__ == "__main__":
    main()
