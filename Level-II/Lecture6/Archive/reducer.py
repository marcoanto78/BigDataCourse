#!/usr/bin/env python
import sys

last_day = None
cntr = 0

for line in sys.stdin:
  day = line.strip()
  if last_day and day != last_day:
    print "%s\t%s" % (last_day, cntr)
    cntr=0
  cntr += 1
  last_day = day
print "%s\t%s" % (last_day, cntr)
