import sys
import os
import time
from multiprocessing import cpu_count, Queue, Process


def move_stream_cursor_to_end_of_first_line(f, chunk):
    f.seek(f.tell() - 1)
    if f.read(1) == "\n":
        return f.tell(), chunk
    b = f.readline()
    return f.tell(), chunk - len(b) - 1


def filter_and_calc(file_name, start, appx_chunk, additional_line_data_lenght, min_age, max_age, keys, results):
    # assume the data has a column called age (number) and a column with gender (F/M)
    # for every age between min_age and max_age, count the number of F and M

    counts = {'f': 0, 'F': 0, 'm': 0, 'M': 0, 'other': 0}
    age_index = keys.index('age')
    gender_index = keys.index('gender')
    genders = frozenset(['f', 'F', 'm', 'M'])

    with open(file_name, 'r') as f:
        f.seek(start)
        actual_start, chunk = move_stream_cursor_to_end_of_first_line(f, appx_chunk)
        read_counter = 0
        for line in f.readlines(chunk):
            read_counter += len(line) + additional_line_data_lenght
            #print("Job ({},{}) - {}".format(actual_start, chunk, line))
            values = line.split(",")
            age = float(values[age_index])
            if min_age <= age <= max_age:
                gender = values[gender_index]
                if gender in genders:
                    counts[gender] += 1
                else:
                    counts['other'] += 1

            if read_counter >= chunk:
                break
    #print("Job ({},{}) DONE".format(actual_start, read_counter))
    counts['f'] += counts.pop('F')
    counts['m'] += counts.pop('M')
    return results.put(counts)


def multi_process_file_processing(file_name, min_age, max_age):
    results = Queue()

    num_of_processes = cpu_count()
    processes = []
    # Calculate args for processes - devide file
    with open(file_name, "r") as f:
        header_line = f.readline()
        additional_line_data_lenght = f.tell() - len(header_line)
        keys = header_line.strip().split(",")
        start = f.tell()
        file_size = os.path.getsize(file_name)
        appx_chunk_size = int((file_size - start) / num_of_processes)
        for i in range(num_of_processes):
            p = Process(target=filter_and_calc,
                        args=[file_name,
                              start,
                              appx_chunk_size,
                              additional_line_data_lenght,
                              min_age,
                              max_age,
                              keys,
                              results])
            processes.append(p)
            # Run process
            p.start()

            if i == num_of_processes - 1:
                appx_chunk_size = -1
            start += appx_chunk_size

    # Exit the completed processes
    for p in processes:
        p.join()

    # Get process results from the output queue
    results = [results.get() for p in processes]

    # merge results
    res_keys = results[0].keys()
    merged_result = dict(zip(res_keys, [0] * len(res_keys)))
    for partial_result in results:
        for k in res_keys:
            merged_result[k] += partial_result[k]
    print(merged_result)


if __name__ == "__main__":
    start_time = time.time()
    file_name = sys.argv[1]
    min_age = float(sys.argv[2])
    max_age = float(sys.argv[3])
    multi_process_file_processing(file_name, min_age, max_age)
    end_time = time.time()
    print("Job took {} seconds".format(end_time - start_time))
