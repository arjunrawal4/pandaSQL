import random
import csv
import argparse
import numpy as np
import string

def generate_integer_csv(rows):
    arr = np.random.randint(low=0, high=100_000_000, size=(rows,16))
    np.savetxt('integer.csv', arr, delimiter=',', fmt='%i', header='c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15', comments='')

def generate_float_csv(rows):
    arr = np.random.uniform(low=0, high=100_000_000, size=(rows, 16))
    np.savetxt('float.csv', arr, delimiter=',', header='c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15', comments='')

def generate_string_csv(rows):
    def get_random_string(len):
        return ''.join([random.choice(string.ascii_letters + string.digits) for n in range(len)])

    with open("string.csv", "w" ) as outf:
        outfile = csv.writer(outf)
        outfile.writerow(['c0','c1','c2','c3','c4','c5','c6','c7','c8','c9','c10','c11','c12','c13','c14','c15'])
        for _ in range(rows):
            row = [get_random_string(12) for i in range(16)]
            outfile.writerow(row)


def generate_join_csv(rows):
    index = np.vstack(np.arange(rows))
    rest = np.random.randint(low=0, high=100_000_000, size=(rows,15))
    arr = np.append(index, rest, axis=1)
    np.savetxt('int_join_a.csv', arr, delimiter=',', fmt='%i', header='c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15', comments='')
    np.savetxt('int_join_b.csv', arr, delimiter=',', fmt='%i', header='c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15', comments='')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', type=str, required=True)
    parser.add_argument('--rows', type=int, required=False, default=10_000_000)
    args = parser.parse_args()

    if args.type == 'int' or args.type == 'integer':
        generate_integer_csv(args.rows)
    if args.type == 'str' or args.type == 'string':
        generate_string_csv(args.rows)
    if args.type == 'fp' or args.type == 'float':
        generate_float_csv(args.rows)
    if args.type == 'join':
        generate_join_csv(args.rows)