import os
import subprocess

class Test():

    def __init__(self, bin_path, data_directory_path, wal_segment_size, connect_count, transaction_count):
        self.bin_path = bin_path
        self.data_directory_path = data_directory_path
        self.wal_segment_size = wal_segment_size
        self.connect_count = connect_count
        self.transaction_count = transaction_count
        
    def initdb(self):
        if os.path.isdir(self.data_directory_path + 'data' + self.wal_segment_size):
            subprocess.call(['sudo', 'rm', '-r', self.data_directory_path + 'data' + self.wal_segment_size])

        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'initdb', '--wal-segsize=' + self.wal_segment_size, '-D', self.data_directory_path + 'data' + self.wal_segment_size])

    def change_max_connections(self, text_to_add, file_name):
        subprocess.call([f'echo "{text_to_add}" | sudo -u postgres tee -a {file_name}'], shell=True)

    def start_server(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pg_ctl', '-D', self.data_directory_path + 'data' + self.wal_segment_size, '-l', self.data_directory_path + 'log' + self.wal_segment_size, 'start'])

    def create_database(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'createdb', 'benchmark'])
        
    def benchmark(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pgbench', '-i', 'benchmark'])
        with open('./many_connections/many_connections_data/many_conn_test' + self.wal_segment_size + '.txt', 'a') as outputfile:
            subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pgbench', '-t', self.transaction_count, '-c', self.connect_count, 'benchmark'], stdout=outputfile)

    def stop_server(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pg_ctl', '-D', self.data_directory_path + 'data' + self.wal_segment_size, '-l', self.data_directory_path + 'log' + self.wal_segment_size, 'stop'])

def test_case(bin_path, data_directory_path, wal_segment_size, transaction_count, tests_count):
    connect_count = ['2', '4', '8', '16', '32', '64', '128', '256', '512']

    for conn in connect_count:
        tran = str(round(transaction_count / int(conn)))
        
        for j in range(tests_count):
            test = Test(bin_path, data_directory_path, wal_segment_size, conn, tran)
            test.initdb()
            if int(conn) > 100:
                text_to_add = "max_connections = 1000"
                file_name = data_directory_path + 'data' + wal_segment_size + '/postgresql.conf'
                test.change_max_connections(text_to_add, file_name)
            test.start_server()
            test.create_database()
            test.benchmark()
            test.stop_server()

            del test

def main():

    # Path to PostgreSQL
    bin_path = '/usr/lib/postgresql/16/bin/'
    # Cluster data path
    data_directory_path = '/usr/local/pgsql/'

    # List of WAL sizes, one element of list - one test case
    wal_segment_size = ['16', '32', '64', '128', '256', '512', '1024']
    # Total transaction count for all test cases
    transaction_count = 1000
    # Total test count for one case
    tests_count = 1
 
    for wss in wal_segment_size:
        test_case(bin_path, data_directory_path, wss, transaction_count, tests_count)

if __name__ == "__main__":
    main()