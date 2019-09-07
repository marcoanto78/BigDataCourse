import random
cnt=100
for x in range(cnt):
  f=random.randint(1,1000)
  seed=random.randint(1,40)
  print str(f) + "," + str(f/2 + seed)
