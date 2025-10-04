
class SimpleLogStructuredDB:
    def __init__(self, filename):
        self.filename = filename
        self.index = {}
        self._load_index()

    def set(self, key, value):
        # log 끝에 추가
        with open(self.filename, "a") as f:
            offset = f.tell()  # fil loc
            record = f"{key}, {value}\n"
            f.write(record)
            self.index[key] = offset

    def get(self, key):
        if key not in self.index:
            return None
        with open(self.filename, "r") as f:
            f.seek(self.index[key])
            line = f.readline()
            _, value = line.strip().split(",")
        return value

    def _load_index(self):
        try:
            with open(self.filename, "r") as f:
                offset = 0
                for line in f:
                    key, _ = line.strip().split(",")
                    self.index[key] = offset
                    offset = f.tell()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    db = SimpleLogStructuredDB('data.log')
    db.set('user:1', 'test1')
    db.set('user:2', 'test2')
    print(db.get('user:1'))
