import sys
import os
import time
from multiprocessing import cpu_count, Queue, Process


def filter_and_calc(stream, min_age, max_age, keys, results):
    # assume the data has a column called age (number) and a column with gender (F/M)
    # for every age between min_age and max_age, count the number of F and M

    counts = {'f': 0, 'F': 0, 'm': 0, 'M': 0, 'other': 0}
    age_index = keys.index('age')
    gender_index = keys.index('gender')
    genders = frozenset(['f', 'F', 'm', 'M'])
    for line in stream:
        values = line.split(",")
        # print("job({}) - {}".format(file_start_position,values))
        age = float(values[age_index])
        if min_age <= age <= max_age:
            gender = values[gender_index]
            if gender in genders:
                counts[gender] += 1
            else:
                counts['other'] += 1

    counts['f'] += counts.pop('F')
    counts['m'] += counts.pop('M')
    return results.put(counts)


def multi_process_file_processing(file_name, min_age, max_age, num_of_processes):
    results = Queue()
    processes = []
    # Calculate args for processes - devide file
    with open(file_name, "r") as f:
        keys = f.readline().strip().split(",")
        start = f.tell()
        file_size = os.path.getsize(file_name)
        appx_chunk_size = int((file_size - start) / num_of_processes)
        for _ in range(num_of_processes):
            p = Process(target=filter_and_calc, args=[f.readlines(appx_chunk_size), min_age, max_age, keys, results])
            processes.append(p)
            # Run process
            p.start()

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
    num_of_processes = int(sys.argv[4])
    multi_process_file_processing(file_name, min_age, max_age, num_of_processes)
    end_time = time.time()
    print("Job took {} seconds".format(end_time - start_time))
