import sys
import csv
import random

file_name = sys.argv[1]

LINES = int(sys.argv[2]) 
print ('Creating file ' + file_name + ' with ' + str(LINES) + ' lines of data')
with open(file_name, 'wb') as csvfile:
    filewriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)

    filewriter.writerow(['age', 'gender', 'country'])
    for i in xrange(0, LINES):
        gender_num = random.randint(1, 101)
        gender = ''
        if gender_num < 45:
            gender = 'F'
        elif gender_num < 90:
            gender = 'M'
        filewriter.writerow([random.randint(1, 101), gender, 'COUNTRY'])
