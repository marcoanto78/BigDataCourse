#!/usr/bin/env python

import sys

#for line in sys.stdin:
#fopen=open("weblog.txt")
#for line in fopen:

for line in sys.stdin:
  day = line.strip().split('[')[1].split(":")[0]
  print day

#fopen.close()
