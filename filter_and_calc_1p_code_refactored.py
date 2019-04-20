# import pandas as pd
import sys
import os
import csv

import time


def filter_and_calc(file_name, min_age, max_age):
    # assume the data has a column called age (number) and a column with gender (F/M)
    # for every age between min_age and max_age, count the number of F and M
    if not os.path.exists(file_name):
        raise ValueError("\nfile_name ', + file_name + ' does not exist\n")

    counts = {'f': 0, 'm': 0, 'other': 0}
    with open(file_name) as csvfile:
        keys = csvfile.readline().strip().split(",")
        age_index = keys.index('age')
        gender_index = keys.index('gender')

        for line in csvfile.readlines():
            values = line.split(",")
            age = float(values[ age_index ])
            if min_age <= age <= max_age:
                gender = values[ gender_index ].lower()
                if gender in counts.keys():
                    counts[ gender ] += 1
                else:
                    counts[ 'other' ] += 1

    return counts


if __name__ == "__main__":
    start = time.time()
    file_name = sys.argv[1]
    min_age = float(sys.argv[2])
    max_age = float(sys.argv[3])
    res = filter_and_calc(file_name, min_age, max_age)
    print(res)
    end = time.time()
    print("Job took {} seconds".format(end - start))