from blackbox import BlackBox
import sys
import random
import time

input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_task = int(sys.argv[3])
output_file = sys.argv[4]
random.seed(553)

seqnum = 0
results = []
time_start = time.time()


bx = BlackBox()
for _ in range(num_task):
    stream_users = bx.ask(input_file, stream_size)
    if _ == 0:
        tmp_lst = [i for i in stream_users]
        n = 100
        seqnum = 100
        res_str = str(seqnum) + "," + tmp_lst[0] + "," + tmp_lst[20] + "," + tmp_lst[40] + "," + tmp_lst[60] + "," + tmp_lst[80] + "\n"
        results.append(res_str)
    else:
        for user in stream_users:
            n += 1
            rand_prob = random.random()
            if rand_prob < seqnum / n:
                index = random.randint(0, 99)
                tmp_lst.pop(index)
                tmp_lst.insert(index, user)
        res_str = str(n) + "," + tmp_lst[0] + "," + tmp_lst[20] + "," + tmp_lst[40] + "," + tmp_lst[60] + "," + tmp_lst[80] + "\n"
        results.append(res_str)



# bx = BlackBox()
# for _ in range(num_task):
#     stream_users = bx.ask(input_file, stream_size)
#     #tmp_lst = []
#     #n = 0
#     if _ == 0:
#         tmp_lst = [i for i in stream_users]

#         seqnum += 100
#         res_str = str(seqnum) + "," + tmp_lst[0] + "," + tmp_lst[20] + "," + tmp_lst[40] + "," + tmp_lst[60] + "," + tmp_lst[80] + "\n"
#         results.append(res_str)
#         n = seqnum
#     else:
#         #seqnum += 100
#         for user in stream_users:
#             n += 1 
#             p = 100 / n
#             if random.random() < p:
#                 index = random.randint(0, 99)
#                 tmp_lst[index] = user
#         n = 100
#         seqnum += 100
#         res_str = str(seqnum) + "," + tmp_lst[0] + "," + tmp_lst[20] + "," + tmp_lst[40] + "," + tmp_lst[60] + "," + tmp_lst[80] + "\n"
#         results.append(res_str)
#print(results)
with open(output_file, "w") as file:
    file.write("seqnum,0_id,20_id,40_id,60_id,80_id\n")
    for i in results:
        file.write(i)

time_end = time.time()
duration = round(time_end - time_start, 2)
print('Duration:', duration)




        
    
# import sys
# import time
# from blackbox import BlackBox
# import random

# if __name__ == "__main__":
#     random.seed(553)
#     # parse command line arguments
#     input_path = sys.argv[1]
#     stream_size = int(sys.argv[2])
#     num_of_asks = int(sys.argv[3])
#     output_path = sys.argv[4]

#     """
#     Start timer
#     """
#     start_time = time.time()
#     bx = BlackBox()

#     # keep track of the sample
#     sample = []
#     # keep track of the number of users arrived so far
#     n = 0
#     # size of the sample
#     s = 100
#     sequence_string = ""
#     for _ in range(num_of_asks):
#         stream_users = bx.ask(input_path, stream_size)
#         if _ > 0:
#             for user in stream_users:
#                 n += 1
#                 prob_keep = random.random()
#                 if prob_keep < s/n:
#                     position = random.randint(0, 99)
#                     sample[position] = user
#         else:
#             for user in stream_users:
#                 sample.append(user)
#             n = 100
#         sequence_string += str(n) + "," + sample[0] + "," + sample[20] + "," + sample[40] + "," + sample[60] + "," + sample[80] + "\n"
#     res = "seqnum,0_id,20_id,40_id,60_id,80_id\n"
#     res += sequence_string
#     with open(output_path, "w") as out_file:
#         out_file.writelines(res)
#     print(res)
#     """
#     Stop timer
#     """
#     duration = time.time() - start_time
#     print("Duration: " + str(duration))