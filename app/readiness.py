import os


class Readiness:
    def __init__(self, readiness_file):
        self.readiness_file = readiness_file
        self._is_ready = False

    def __enter__(self):
        self.make_ready()

    def __exit__(self, *args):
        self.make_unready()

    def make_ready(self):
        with open(self.readiness_file, 'w'):
            self._is_ready = True

    def make_unready(self):
        os.remove(self.readiness_file)
        self._is_ready = False
