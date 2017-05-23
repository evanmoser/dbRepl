import pyodbc

class Db(object):
    """Database connector (requires ODBC)"""
    def __init__(self, dsn):
        self.DSN = "DSN={0}".format(dsn)
        self.conn = pyodbc.connect(self.DSN)
        self.cursor = self.conn.cursor()

    def getHeaders(self):
        cols = [column[0] for column in self.cursor.description]
        return cols

    def query(self, query):
        self.cursor.execute(query)

    def qfo(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()

    def qfa(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def disconnect(self):
        self.cursor.close()
        self.conn.commit()
        self.conn.close()
