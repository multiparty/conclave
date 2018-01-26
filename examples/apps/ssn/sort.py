#!/usr/bin/python
import sys

if len(sys.argv) < 2:
  print("usage: sort.py <input file>")
  sys.exit(1)

f = open(sys.argv[1])
lines = []
for l in f.readlines():
  fields = l.split(",")
  lines.append((int(fields[0]), int(fields[1])))

for (k, v) in sorted(lines, key=lambda x: x[0]):
  print("{},{}".format(k, v))
