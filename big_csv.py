class CSVReaderThingy:
    def __init__(self, filename: str):
        self.filename: str = filename
        self.index: dict[int, int] = {}

    def get_column_names(self) -> list(str):
        with open(self.filename, "r") as f:
            title_list: list[str] = f.readline().split(",")
        return title_list

    def make_index(self, page_size: int):
        """Index will start with the first non-title line at 0"""
        with open(self.filename, "r") as f:
            f.readline().split(",")  # skip title
            line_counter = 0
            char_number: int = 0
            index = {}
            while line := f.readline():
                char_number += len(line) + 1  # Add one for newline character
                if line % page_size == 0:
                    index[line] = char_number
