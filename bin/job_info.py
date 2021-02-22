class Table:
    def __init__(self, name, suffix, path, result):
        self.name = name
        self.suffix = suffix
        self.path = path
        self.result = result
    
    def __repr__(self):
        return repr((self.name, self.suffix, self.path, self.result))

class Database:
    def __init__(self, name, type, tables):
        self.name = name
        self.type = type
        self.tables = tables
    
    def __repr__(self):
        return repr((self.name, self.type, self.tables))

class Job:
    def __init__(self, id, type, date, hour, databases):
        self.id = id
        self.type = type
        self.date = date
        self.hour = hour
        self.databases = databases

    def __repr__(self):
        return repr((self.id, self.type, self.date, self.hour, self.databases))