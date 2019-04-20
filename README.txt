Hello programmer,
This is a small task about running python tasks in parallel (several simultaneous processes).

We are researching data about a population. The utility attached counts the number of people between ages split by gender. The task at hand is to optimize the utility's runtime.

In the attached zip you will find 4 files:
1. This README file
2. filter_and_calc.py - actual python code
3. data.csv - a sample file of 100 lines of data
4. create_data.py - a python script to generate larger data files on which to test the parallel solution 


filter_and_calc is a function that takes as input: a file name, the minimum age and the maximum age.
It returns a count of records that fit the criteria (between ages) grouped by gender.
The file contains the naive implementation.

Please find a way to run this as fast as possible. A good place to start is by executing in parallel. This should prove significant assuming the file is big (1 million records or more).
Execution time should be minimal and the result should be the same when ran in a single thread.

Benchmark:
For a file of size 10M lines, The naive solution took 18.095 seconds and an improved and parallel (with 4 cores) solution took about 3 seconds.

Your task:
1. Reimplement filter_and_calc so that it uses a given number of processes (default 4). Implement with no external dependencies (only basic python and shell if needed)
2. Bonus: use a framework that helps generalize running in parallel (and maybe running the aggregation at the end).
    You can google "Python pipeline frameworks"


1.refactor for the performance of the python code?
1.can I use multiprocessing python
1.cpu_count is not better?

2.luigi?

team8



