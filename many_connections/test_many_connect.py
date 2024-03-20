import os
import re
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

    def change_parameters(self, change_checkpoint_timeout, change_max_wal_size, file_name):
        subprocess.call([f'echo "{change_checkpoint_timeout}" | sudo -u postgres tee -a {file_name}'], shell=True)
        subprocess.call([f'echo "{change_max_wal_size}" | sudo -u postgres tee -a {file_name}'], shell=True)

    def kill_port(self):
        subprocess.call(['lsof -i :5432 -t'], shell=True)

    def start_server(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pg_ctl', '-D', self.data_directory_path + 'data' + self.wal_segment_size, '-l', self.data_directory_path + 'log' + self.wal_segment_size, 'start'])

    def create_database(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'createdb', 'benchmark'])

    def benchmark(self):
        subprocess.call(['sudo', 'i', '-u', 'postgres', self.bin_path + 'psql', '-c', 'CHECKPOINT'])
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pgbench', '-i', 'benchmark'])
        with open('/home/kosenkovaan/python_projects/pgbench_test-1/many_connections/many_connections_data/many_conn_test' + self.wal_segment_size + '.txt', 'a') as outputfile:
            subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pgbench', '-t', self.transaction_count, '-c', self.connect_count, 'benchmark'], stdout=outputfile)

    def stop_server(self):
        subprocess.call(['sudo', '-u', 'postgres', self.bin_path + 'pg_ctl', '-D', self.data_directory_path + 'data' + self.wal_segment_size, '-l', self.data_directory_path + 'log' + self.wal_segment_size, 'stop'])

def test_case(bin_path, data_directory_path, transaction_count, test_number, segment_size, connect):
    tran = str(round(transaction_count / int(connect)))

    test = Test(bin_path, data_directory_path, segment_size, connect, tran)
    test.initdb()
    if int(connect) > 100:
        text_to_add = "max_connections = 1000"
        file_name = data_directory_path + 'data' + segment_size + '/postgresql.conf'
        test.change_max_connections(text_to_add, file_name)
    change_checkpoint_timeout = "checkpoint_timeout = 7200"
    change_max_wal_size = "max_wal_size = 102400"
    file_name = data_directory_path + 'data' + segment_size + '/postgresql.conf'
    test.change_parameters(change_checkpoint_timeout, change_max_wal_size, file_name)
    test.kill_port()
    test.start_server()
    test.create_database()
    test.benchmark()
    test.stop_server()
        
    del test
    #pass

def logging(test, segment_size, connect):
    with open('/home/kosenkovaan/python_projects/pgbench_test-1/many_connections/many_connections_data/logfile.txt', 'a') as file:
        file.write('\nTest:' + test + ', Segment size:' + segment_size + ', Connect:' + connect)

def read_log(file_path):
    try:
        with open(file_path, 'r') as file:
            last_line = file.readlines()[-1]

        pattern = re.compile(r'Test:(\d+), Segment size:(\d+), Connect:(\d+)')
        match = pattern.match(last_line)

        if match:
            test_number = match.group(1)
            segment_size = match.group(2)
            connect = match.group(3)

    except FileNotFoundError:
        print('file not found')
    except IOError:
        print('Error while reading file')

    return test_number, segment_size, connect

def is_file_empty(file_path):
    if os.path.getsize(file_path) > 0:
        return False
    else:
        return True

def main():
    bin_path = '/usr/lib/postgresql/16/bin/'            # Path to PostgreSQL
    data_directory_path = '/usr/local/pgsql/'           # Cluster data path

    wal_segment_size = ['16', '32', '64', '128', '256', '512', '1024']              # List of WAL sizes
    connect_count = ['2', '4', '8', '16', '32', '64', '128', '256', '512']          # List of connections

    transactions_count = 10000               # Total transaction count for all test cases
    tests_count = 2                         # Total test count for one case

    file_path = '/home/kosenkovaan/python_projects/pgbench_test-1/many_connections/many_connections_data/logfile.txt'

    if is_file_empty(file_path):
        start_index_test = 1
        for test_index in range(start_index_test, tests_count + 1):
            for segment_size_index in range(0, len(wal_segment_size)):
                for connect_index in range(0, len(connect_count)):
                    test_case(bin_path, data_directory_path, transactions_count, test_index, wal_segment_size[segment_size_index], connect_count[connect_index])
                    logging(str(test_index), str(wal_segment_size[segment_size_index]), str(connect_count[connect_index]))
    else:
        test_number, segment_size, connect = read_log(file_path)
        last_test_index = int(test_number)
        last_connect_index = connect_count.index(connect)
        last_wal_index = wal_segment_size.index(segment_size)

        if last_test_index == tests_count:
            if last_wal_index == len(wal_segment_size) - 1:
                if last_connect_index == len(connect_count) - 1:
                    pass
                else:
                    for connect_index in range(last_connect_index + 1, len(connect_count)):
                        test_case(bin_path, data_directory_path, transactions_count, last_test_index, wal_segment_size[last_wal_index], connect_count[connect_index])
                        logging(str(last_test_index), str(wal_segment_size[last_wal_index]), str(connect_count[connect_index]))
            else:
                for segment_size_index in range(last_wal_index, len(wal_segment_size)):
                    if last_connect_index == len(connect_count) - 1:
                        last_connect_index = 0
                    else:
                        for connect_index in range(last_connect_index + 1, len(connect_count)):
                            test_case(bin_path, data_directory_path, transactions_count, test_index, wal_segment_size[segment_size_index], connect_count[connect_index])
                            logging(str(test_index), str(wal_segment_size[segment_size_index]), str(connect_count[connect_index]))
                        last_wal_index = 0
                        last_connect_index = 0
        else:        
            for test_index in range(last_test_index, tests_count + 1):
                if last_wal_index == len(wal_segment_size) - 1:
                    if last_connect_index == len(connect_count) - 1:
                        last_connect_index = 0
                    else:
                        for connect_index in range(last_connect_index + 1, len(connect_count)):
                            test_case(bin_path, data_directory_path, transactions_count, test_index, wal_segment_size[segment_size_index], connect_count[connect_index])
                            logging(str(test_index), str(wal_segment_size[segment_size_index]), str(connect_count[connect_index]))
                        last_wal_index = 0
                        last_connect_index = 0
                else:
                    for segment_size_index in range(last_wal_index, len(wal_segment_size)):
                        if last_connect_index == len(connect_count) - 1:
                            last_connect_index = 0
                        else:
                            for connect_index in range(last_connect_index, len(connect_count)):
                                test_case(bin_path, data_directory_path, transactions_count, test_index, wal_segment_size[segment_size_index], connect_count[connect_index])
                                logging(str(test_index), str(wal_segment_size[segment_size_index]), str(connect_count[connect_index]))
                            last_wal_index = 0
                            last_connect_index = 0      

if __name__ == "__main__":
    main()