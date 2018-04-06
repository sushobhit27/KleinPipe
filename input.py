class Input(object):
    def __init__(self, data):
        self.data = []
        if data is not None:
            self.data.extend(data)

    def add(self, data):
        if data is not None:
            self.data.extend(data)

    def __str__(self):
        return str(self.data)