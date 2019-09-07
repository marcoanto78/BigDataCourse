import random
import string
cnt=1
N=6
while cnt < 1000000:
  loop_iter=0
  data_str = str(cnt)
  while loop_iter<35:
    data_str += "," + str(float(random.randint(93, 573))/100) + "," + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))
    loop_iter += 1
  print data_str
  cnt += 1
