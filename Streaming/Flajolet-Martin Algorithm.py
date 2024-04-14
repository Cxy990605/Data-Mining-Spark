from blackbox import BlackBox
import sys
import random
import binascii
import time
import statistics


input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_task = int(sys.argv[3])
output_file = sys.argv[4]
random.seed(553)

results = []

lst = random.sample(range(30000, 50000),20)
a = lst[:10]
b = lst[10:]
zipped = list(zip(a,b))
m = 69997
p =  11299
def myhashs(s):
    result = []
    for i in range(len(zipped)):
        str2num = int(binascii.hexlify(s.encode('utf8')),16)
        hash_func = ((zipped[i][0] * str2num + zipped[i][0]) % p) % m 
        result.append(hash_func)
    return result

def implement_FM(users):
    sum_ground = 0
    sum_estimate = 0
    max_trailing_zeros = [0] * 10
    ground_truth = len(set(users))
    
    for user in users:
        tmp_zeros = []
        tmp_hash = myhashs(user)
        for value in tmp_hash:
            num2bin = bin(value)
            length = len(num2bin) - len(num2bin.rstrip("0"))
            tmp_zeros.append(length)
        max_trailing_zeros = [max(i,j) for i,j in zip(max_trailing_zeros, tmp_zeros)]

    unique_esitmates = sorted([2**r for r in max_trailing_zeros])
    avg = []
    for step in range(0,10,10):
        avg.append(statistics.mean(unique_esitmates[step:(step+10)]))
    median_estimate = round(statistics.median(avg))
    
    output_res = str(ground_truth) + "," + str(median_estimate)
    results.append(output_res)
    sum_ground += ground_truth
    sum_estimate += median_estimate


time_start = time.time()

bx = BlackBox()
for _ in range(num_task):
    stream_users = bx.ask(input_file, stream_size)
    implement_FM(stream_users)


with open(output_file, "w") as file:
    file.write("Time,Ground Truth,Estimation\n")
    for idx in range(len(results)):
        file.write(str(idx))
        file.write(",")
        file.write(results[idx])
        file.write("\n")

    
time_end = time.time()
duration = round(time_end - time_start, 2)
print('Duration:', duration)