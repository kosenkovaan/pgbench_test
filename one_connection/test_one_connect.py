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
            subprocess.call(['sudo', '-i', 'rm', '-r', self.data_directory_path + 'data' + self.wal_segment_size])

        subprocess.call(['sudo', '-i', '-u', 'postgres', self.bin_path + 'initdb', '--wal-segsize=' + self.wal_segment_size, '-D', self.data_directory_path + 'data' + self.wal_segment_size])

    def start_server(self):
        subprocess.call(['sudo', '-i', '-u', 'postgres', self.bin_path + 'pg_ctl', '-D', self.data_directory_path + 'data' + self.wal_segment_size, '-l', self.data_directory_path + 'log' + self.wal_segment_size, 'start'])

    def create_database(self):
        subprocess.call(['sudo', '-i', '-u', 'postgres', self.bin_path + 'createdb', 'benchmark'])
        
    def benchmark(self):
        subprocess.call(['sudo', 'i', '-u', 'postgres', self.bin_path + 'psql', '-c', 'CHECKPOINT'])
        subprocess.call(['sudo', '-i', '-u', 'postgres', self.bin_path + 'pgbench', '-i', 'benchmark'])
        with open('./one_connection/data/one_conn_test' + self.wal_segment_size + '.txt', 'a') as outputfile:
            subprocess.call(['sudo', '-i', '-u', 'postgres', self.bin_path + 'pgbench', '-t', self.transaction_count, '-c', self.connect_count, 'benchmark'], stdout=outputfile)

    def stop_server(self):
        subprocess.call(['sudo', '-i', '-u', 'postgres', self.bin_path + 'pg_ctl', '-D', self.data_directory_path + 'data' + self.wal_segment_size, '-l', self.data_directory_path + 'log' + self.wal_segment_size, 'stop'])

def test_case(bin_path, data_directory_path, wal_segment_size, transaction_count, test_count):
    
    # List of cases by number of connections
    connect_count = ['1']
 
    for conn in connect_count:
        # Find number of transaction per connection
        tran = str(round(transaction_count / int(conn)))
        
        for j in range(test_count):
            test = Test(bin_path, data_directory_path, wal_segment_size, conn, tran)
            test.initdb()
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
    wal_segment_size = ['1', '2', '4', '8', '16', '32', '64', '128', '256', '512', '1024']
    # Total transaction count for all test cases
    transaction_count = 1000000
    # Total test count for one case
    test_count = 5
 
    for wss in wal_segment_size:
        test_case(bin_path, data_directory_path, wss, transaction_count, test_count)

if __name__ == "__main__":
    main()