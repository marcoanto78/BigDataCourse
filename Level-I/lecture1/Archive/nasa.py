fopen = open("D:\Shyam\Today\sample_data.txt")

counter = dict()
for line in fopen:
  dt = line.split("[")[1].split(":")[0]
  if counter.get(dt) != None:
    counter[dt] = counter.get(dt) + 1
  else:
    counter[dt] = 1

print(counter)
fopen.close()