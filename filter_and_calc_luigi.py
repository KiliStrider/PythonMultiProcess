import json
import luigi
import os
import sys
import time


class FilterAndCount(luigi.Task): # standard Luigi Task class
    min_age = luigi.FloatParameter()
    max_age = luigi.FloatParameter()
    keys = luigi.ListParameter()
    lines = luigi.ListParameter()
    index = luigi.IntParameter()

    def run(self):
        counts = {"f": 0, "F": 0, "m": 0, "M": 0, "other": 0}
        age_index = self.keys.index('age')
        gender_index = self.keys.index('gender')
        genders = frozenset(['f', 'F', 'm', 'M'])

        for line in self.lines:
            values = line.split(",")
            age = float(values[age_index])
            if self.min_age <= age <= self.max_age:
                gender = values[gender_index]
                if gender in genders:
                    counts[gender] += 1
                else:
                    counts['other'] += 1

        counts['f'] += counts.pop('F')
        counts['m'] += counts.pop('M')

        # write the results (this is where you could use hdfs/db/etc)
        with self.output().open('w') as f:
            f.write(json.dumps(counts))

    def output(self):
        return luigi.LocalTarget('partial_count_results.{}.txt'.format(self.index))


class MergeResults(luigi.Task): # standard Luigi Task class
    file = luigi.Parameter()
    num_of_processes = luigi.IntParameter(default=4)
    min_age = luigi.FloatParameter()
    max_age = luigi.FloatParameter()

    #def requires(self):
    #    return []

    def run(self):
        with open(self.file, "r") as f:
            keys = f.readline().strip().split(",")
            start = f.tell()
            file_size = os.path.getsize(file_name)
            appx_chunk_size = int((file_size - start) / self.num_of_processes)
            tasks = []
            for i in range(self.num_of_processes):
                if i == self.num_of_processes - 1:
                    appx_chunk_size = -1
                tasks.append(FilterAndCount(index=i,
                                            lines=f.readlines(appx_chunk_size),
                                            min_age=min_age,
                                            max_age=max_age,
                                            keys=keys))

        targets = yield tasks
        results = []
        for target in targets:
            with target.open() as t:
                results.append(json.loads(t.read().strip()))

        # merge results
        res_keys = results[0].keys()
        merged_result = dict(zip(res_keys, [0] * len(res_keys)))
        for partial_result in results:
            for k in res_keys:
                merged_result[k] += partial_result[k]

        # write the results (this is where you could use hdfs/db/etc)
        with self.output().open('w') as f:
            f.write(json.dumps(merged_result))

    def output(self):
        return luigi.LocalTarget('counts.txt')


if __name__ == '__main__':
    start = time.time()
    file_name = sys.argv[1]
    min_age = sys.argv[2]
    max_age = sys.argv[3]
    num_of_processes = sys.argv[4]
    luigi.run(["--local-scheduler", "--workers", "1", "--MergeResults-num-of-processes", num_of_processes, "--MergeResults-file", file_name,  "--MergeResults-min-age", min_age, "--MergeResults-max-age", max_age],
              main_task_cls=MergeResults)
    end = time.time()
    print("Job took {} seconds".format(end - start))
