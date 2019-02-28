#!/usr/bin/python
import random
import sys

import gflags

random.seed(42)
from faker import Faker

FLAGS = gflags.FLAGS
gflags.DEFINE_bool('realistic', True, 'Realistic output data.')
gflags.DEFINE_bool('headers', True, 'Add column headers.')
gflags.DEFINE_integer('companies', 2, 'Number of credit card companies.')
gflags.DEFINE_integer('scale', 1000, 'Scale factor.')
gflags.DEFINE_string('output', 'ssn_data', 'Output file prefix.')


def usage():
    print('Usage: %s ARGS\\n%s' % (sys.argv[0], FLAGS))


def main(argv):
    try:
        argv = FLAGS(argv)
    except gflags.FlagsError as e:
        print(e)
        usage()
        sys.exit(1)

    f = Faker()
    f.seed(42)
    # regulator data: (name, address, zip, ssn)
    rf = open("{}/govreg.csv".format(FLAGS.output), "w")
    if FLAGS.headers:
        rf.write("a,b\n")
    ssns = set()
    for i in range(int(FLAGS.scale * 1.5)):
        if FLAGS.realistic:
            ssns.add(f.ssn())
        else:
            ssns.add(f.ssn().replace("-", ""))

    people = set()
    for i in range(FLAGS.scale):
        ssn = random.sample(ssns, 1)[0]
        while ssn in people:
            ssn = random.sample(ssns, 1)[0]
        people.add(ssn)
        if FLAGS.realistic:
            rf.write("\"{}\",\"{}\",{},{}\n".format(f.name(), f.street_address(), f.zipcode(), ssn))
        else:
            rf.write("{},{}\n".format(ssn, f.zipcode()))
    rf.close()

    # credit card company data
    for c in range(FLAGS.companies):
        ccf = open("{}/company{}.csv".format(FLAGS.output, c), "w")
        if FLAGS.headers:
            ccf.write("c,d\n")
        for i in range(FLAGS.scale):
            ssn = random.sample(ssns, 1)[0]
            rating = random.randint(400, 800)
            if FLAGS.realistic:
                ccf.write("{},{},{}\n".format(ssn, f.credit_card_number(), rating))
            else:
                ccf.write("{},{}\n".format(ssn, rating))
        ccf.close()


if __name__ == '__main__':
    main(sys.argv)
