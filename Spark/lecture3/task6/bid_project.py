import sys,os
from random import shuffle
import math
bidders=["Jing", "Ming", "Achintya", "Jagtar", "Alex", "Mustafa", "Lurie", "Shyam"]

randomize = int(sys.argv[1])
while randomize>0:
  shuffle(bidders)
#  print bidders
  randomize -= 1

half = int(math.floor(len(bidders)/2))
one = bidders[:half]
two = bidders[half:]
cnt=0
if half*2==len(bidders):
  while cnt<half:
    print "\n\n############################# BID " + str(cnt+1) + " #############################"
    print "\nPARTICIPANTS ARE:  " + one[cnt] + " & " + two[cnt]
    print "\n#################################################################\n"
    cnt += 1
    raw_input("Shall I declare the next candidates to bid?:")
else:
  while cnt<half:
    print "\n\n############################# BID " + str(cnt+1) + " #############################"
    if cnt == len(one)-1:
      print "\nPARTICIPANTS ARE:  " + one[cnt] + " , " + two[cnt] + " & " + two[cnt+1]
    else:
      print "\nPARTICIPANTS ARE:  " + one[cnt] + " & " + two[cnt]
    cnt += 1
    print "\n#################################################################\n"
    raw_input("Shall I declare the next candidates to bid?:")
