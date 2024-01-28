

wal_segment_size = ['16', '32', '64', '128', '256', '512', '1024']
s = []
result = {}

for wss in wal_segment_size:
    result[wss] = {}
    with open('many_conn_test' + wss + '.txt', 'r') as outputfile:
        s.append(outputfile.readlines())

cur_file = 0

for file in s:
    local_result = {}
    for line in file:
        if line[0:7] == "pgbench":
            if not local_result:
                continue
            if 'number_of_clients' in local_result:
                number_of_clients = int(local_result['number_of_clients'])
                cur_val_seg = wal_segment_size[cur_file]
                if not result[cur_val_seg] or not number_of_clients in result[cur_val_seg]:
                    result[cur_val_seg][number_of_clients] = []
                result[cur_val_seg][number_of_clients].append({})
                cur_len = len(result[cur_val_seg][number_of_clients])
                for field_name, value in local_result.items():
                    result[cur_val_seg][number_of_clients][cur_len - 1][field_name] = value
            else:
                print('Error')
            continue
        key_value = line.split(':', 1)
        if len(key_value) == 1:
            key_value = line.split('=', 1)
            key_value[0] = key_value[0][:-1]
        local_result[key_value[0].replace(' ', '_')] = key_value[1][1:-1]
        if key_value[0] == 'tps':
            local_result['tps'] = float(key_value[1].split(' ')[1])
    if 'number_of_clients' in local_result:
        number_of_clients = int(local_result['number_of_clients'])
        cur_val_seg = wal_segment_size[cur_file]
        if not result[cur_val_seg] or not number_of_clients in result[cur_val_seg]:
            result[cur_val_seg][number_of_clients] = []
        result[cur_val_seg][number_of_clients].append({})
        cur_len = len(result[cur_val_seg][number_of_clients])
        for field_name, value in local_result.items():
            result[cur_val_seg][number_of_clients][cur_len - 1][field_name] = value
    cur_file = cur_file + 1

fin_table = {}

for wss in wal_segment_size:
    fin_table[wss] = {}

for wss, client_data in result.items():
    for conn_count, run_data in client_data.items():
        if not conn_count in fin_table[wss]:
            fin_table[wss][conn_count] = 0
        for cur_run in run_data:
            fin_table[wss][conn_count] = fin_table[wss][conn_count] + cur_run['tps']
        fin_table[wss][conn_count] = fin_table[wss][conn_count] / len(run_data)
        print('wss: ' + wss + "; conn_count: ", conn_count, "; avg_tps: ", fin_table[wss][conn_count], "; run_count: ", len(run_data))