import os

import psycopg2


class Timescale:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ.get("TS_HOST"),
            port=os.environ.get("TS_PORT"),
            user=os.environ.get("TS_USER"),
            password=os.environ.get("TS_PASSWORD"),
            database=os.environ.get("TS_DBNAME"))
        self.cursor = self.conn.cursor()

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS sensor_data (
            time timestamp DEFAULT NOW() PRIMARY KEY ,
            name VARCHAR(255) NOT NULL,
            temperature FLOAT,
            humidity FLOAT,
            velocity FLOAT,
            battery_level FLOAT NOT NULL,
            last_seen timestamp NOT NULL
        );
        """
        self.execute(query)
        self.conn.commit()

    def get_cursor(self):
        return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()

    def ping(self):
        return self.conn.ping()

    def execute(self, query, values=None):
        if values:
            self.cursor.execute(query, values)
        else:
            self.cursor.execute(query)
        self.conn.commit()

    def delete(self, table):
        self.cursor.execute("DELETE FROM " + table)
        self.conn.commit()