from cassandra.cluster import Cluster

KEY_SPACE = "sensor"


class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts, protocol_version=4)
        self.session = self.cluster.connect()

    def create_tables(self):
        replication_config = {'class': 'SimpleStrategy', 'replication_factor': 1}
        self.session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {KEY_SPACE} WITH replication = {replication_config}")
        self.session.set_keyspace(KEY_SPACE)
        self.session.execute(
            "CREATE TABLE IF NOT EXISTS low_battery (battery_level float,name text,PRIMARY KEY (battery_level, name));")
        self.session.execute(
            "CREATE TABLE IF NOT EXISTS type_sensor (sensor_type text, id int, PRIMARY KEY (sensor_type, id));")
        self.session.execute(
            "CREATE TABLE IF NOT EXISTS temperature_data ( time timestamp, name text, temperature float, PRIMARY KEY (time, name));")

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query, values=None):
        if values:
            return self.session.execute(query, values)
        else:
            return self.session.execute(query)