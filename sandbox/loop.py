import time
def update_time(t):
    new_t = time.time()
    print(new_t-t)
    return new_t 

t = time.time()

acc = 1
for i in range(2,2000*5*52*20*24):
    acc *= i
    acc /= (i-1)
    acc += 1
print(acc)

t = update_time(t)
# 56 seconds