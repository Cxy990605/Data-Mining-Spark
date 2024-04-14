import binascii
#import re
import sys
import random
import time
from blackbox import BlackBox

#### commandline: /usr/bin/python3 /Users/xiangyuanchi/Downloads/TestProgram/task1.py <input_filename> stream_size num_of_asks <output_filename>
#### /usr/bin/python3 /Users/xiangyuanchi/Downloads/TestProgram/task1.py /Users/xiangyuanchi/Downloads/TestProgram/users.txt 100 30 /Users/xiangyuanchi/Downloads/TestProgram/output.csv


input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_task = int(sys.argv[3])
output_file = sys.argv[4]
random.seed(553)


lst = random.sample(range(30000, 50000),20)
a = lst[:10]
b = lst[10:]
zipped = list(zip(a,b))


#### Build hash functions bucket: f(x) = ((ax + b) % p) % m
m = 69997
p =  11299
def myhashs(s):
    result = []
    for i in range(len(zipped)):
        str2num = int(binascii.hexlify(s.encode('utf8')),16)
        hash_func = ((zipped[i][0] * str2num + zipped[i][0]) % p) % m 
        result.append(hash_func)
    return result


def compute_fpr(input_FP, input_TN):
    return input_FP/(input_FP + input_TN)


time_start = time.time()
bx = BlackBox()
results = []
visited = set()
bit_array = [0] * m 

for _ in range(num_task):
    idx_line = 0 
    stream_users = bx.ask(input_file, stream_size)
    FP = 0
    TN = 0
    seen = False
    for user in stream_users:
        hash_index = myhashs(user)
        for index in hash_index:
            if bit_array[index] == 1:
                seen = True
            else:
                seen = False
                TN += 1
                break

        if seen == True:
            if user not in visited:
                FP += 1
        for index in hash_index:
            bit_array[index] = 1
        visited.add(user)
        
    if TN == 0 and FP == 0:
        fpr = 0
    else:
        fpr = compute_fpr(FP, TN)
    tmp = str(idx_line) + "," + str(fpr)
    results.append(tmp)
    idx_line += 1
    
with open(output_file, "w") as file:
    file.write("Time,FPR\n")
    for lines in results:
        file.write(lines)
        file.write("\n")
        
time_end = time.time()
duration = round(time_end - time_start, 2)
print('Duration:', duration)