import luigi
import sys
import time


class ReadCSV(luigi.ExternalTask):
    file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.file)


class Count(luigi.Task): # standard Luigi Task class
    min_age = luigi.FloatParameter()
    max_age = luigi.FloatParameter()
    file = luigi.Parameter()

    def requires(self):
        # we need to read the log file before we can process it
        return ReadCSV(file=self.file)

    def run(self):
        # use the file passed from the previous task
        with self.input().open() as f:
            keys = f.readline().strip().split(",")
            counts = {'f': 0, 'F': 0, 'm': 0, 'M': 0, 'other': 0}
            age_index = keys.index('age')
            gender_index = keys.index('gender')
            genders = frozenset(['f', 'F', 'm', 'M'])

            for line in f:
                values = line.split(",")
                # print("job({}) - {}".format(file_start_position,values))
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
            f.write(str(counts))

    def output(self):
        return luigi.LocalTarget('count_results.txt')


if __name__ == '__main__':
    start = time.time()
    file_name = sys.argv[1]
    min_age = sys.argv[2]
    max_age = sys.argv[3]
    luigi.run(["--local-scheduler", "--workers", "4", "--file", file_name,  "--Count-min-age", min_age, "--Count-max-age", max_age],
              main_task_cls=Count)
    end = time.time()
    print("Job took {} seconds".format(end - start))
