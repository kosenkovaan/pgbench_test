wal_segment_size = ['16', '32']
s = []
result = {}
j = 0

for wss in wal_segment_size:
    result[wss] = {}
    with open('test' + wss + '.txt', 'r') as outputfile:
        s[j] = outputfile.read()

for file in s:
    i = -1
    cur_file = 0
    local_result = {}
    for line in file:
        if line[0:7] == "pgbench":
            i = i + 1
            if not local_result:
                continue
            if 'number_of_clients' in local_result:
                number_of_clients = local_result['number_of_clients']
                cur_val_seg = wal_segment_size[cur_file]
                result[cur_val_seg][number_of_clients] = {}
                for field_name, value in local_result:
                    result[cur_val_seg][number_of_clients][field_name] = value
            else:
                print('Error')
            continue
        key_value = line.split(':')
        local_result[key_value[0].replace(' ', '_')] = key_value[1][1:]
