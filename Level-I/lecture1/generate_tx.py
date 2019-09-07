import random
cnt=100000
product=dict()
with open("product.csv") as f:
  for line in f:
    tkns=line.split(",")
    product[int(tkns[0])]=tkns[1].strip(" ")
#print product
for x in range(cnt):
  f=random.randint(1,95)
  qty=random.randint(1,10)
  amt=random.randint(1,100)
  print str(x) + "," + product.get(f) + "," + str(qty) + "," + str(amt) + ",2019-02-02 12:12:12"
