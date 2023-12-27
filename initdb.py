import os
import subprocess

class Test():

    def __init__(self, bin_path, data_directory_path, wal_segsize, conn_count, tran_count):
        self.bin_path = bin_path
        self.data_directory_path = data_directory_path
        self.wal_segsize = wal_segsize
        self.conn_count = conn_count
        self.tran_count = tran_count
        
    def initdb(self):
        if os.path.isdir(self.data_directory_path + 'data' + self.wal_segsize):
            subprocess.call(['sudo', 'rm', '-r', self.data_directory_path + 'data' + self.wal_segsize])

        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'initdb', '--wal-segsize=' + self.wal_segsize, '-D', self.data_directory_path + 'data' + self.wal_segsize])

    def start_server(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pg_ctl', '-D', self.data_directory_path + 'data' + self.wal_segsize, '-l', self.data_directory_path + 'log' + self.wal_segsize, 'start'])

    def create_database(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'createdb', 'benchmark'])
        
    def benchmark(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pgbench', '-i', 'benchmark'])
        with open('test' + self.wal_segsize + '.txt', 'a') as outputfile:
            subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pgbench', '-t', self.tran_count, '-c', self.conn_count, 'benchmark'], stdout=outputfile)

    def stop_server(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pg_ctl', '-D', self.data_directory_path + 'data' + self.wal_segsize, '-l', self.data_directory_path + 'log' + self.wal_segsize, 'stop'])

def test_case(bin_path, data_directory_path, wal_segment_size, tran_count):
    connect_count = ['2', '4', '8']

    for conn in connect_count:
        t = str(round(tran_count / int(conn)))
        for j in range(3):
            test = Test(bin_path, data_directory_path, wal_segment_size, conn, t)
            test.initdb()
            test.start_server()
            test.create_database()
            test.benchmark()
            test.stop_server()
            del test

def main():
    bin_path = '/usr/lib/postgresql/16/bin/'
    data_directory_path = '/usr/local/pgsql/'
    wal_segment_size = ['16', '32']
    transaction_count = 100
 
    for wss in wal_segment_size:
        test_case(bin_path, data_directory_path, wss, transaction_count)

if __name__ == "__main__":
    main()