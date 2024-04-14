import sys
from pyspark import SparkContext
import os
import time
from itertools import combinations
import math
from collections import defaultdict

# environment setting
# os.environ["SPARK_HOME"] = "/Applications/spark-3.1.2-bin-hadoop3.2"
# os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3.6"


def add_fc(a,b):
    return a+b

def single(basket_l, threshold):
    info = defaultdict(int)
    final_candidate = []
    
    for single_basket in basket_l:
        for item in single_basket:
            info[item] += 1
    for cand in info:
        if info[cand] >= threshold:
            final_candidate.append(cand)
    return sorted(final_candidate)


def apriori(basket_l, threshold, target_number):
    if target_number == 1:
        return single(basket_l, threshold)
    else:
        prev_frequent = single(basket_l,threshold)
        huge_info = sorted([(i,) for i in prev_frequent])
        flag = target_number

        while flag > 1:
            combo_number = target_number-flag+2
            
            if combo_number == 2:
                stage_freq_single = prev_frequent
            else:
                stage_freq_single = set()
                for combo in prev_frequent:
                    for item in combo:
                        stage_freq_single.add(item)
                stage_freq_single = sorted(stage_freq_single)
            
            info = defaultdict(int)        
            new_comb = combinations(stage_freq_single, combo_number)
            for combs in new_comb:
                for b in basket_l:
                    pass_lst = [i for i in combs if i in b]
                    if set(pass_lst) == set(combs):
                        info[combs] += 1
        
            final_candidate = []
            for candidate in info.keys():
                if info[candidate] >= threshold:
                    final_candidate.append(candidate)
            final_candidate = [tuple(sorted(i)) for i in final_candidate]
        
            final_candidate = sorted(list(set(final_candidate)))
            
            if len(final_candidate) != 0:
                huge_info = huge_info + final_candidate
            flag -= 1
            prev_frequent = final_candidate
        return huge_info


def implement_son(bsk,total_length,num):
    maxx = max([len(i) for i in bsk])
    sample_threshold = math.ceil(len(bsk) * num / total_length)
    return apriori(bsk,sample_threshold,maxx)


def output(medium_input):
    st, end = len(medium_input[0]), len(medium_input[-1])
    cache = defaultdict(list)
    for i in range(st,end+1):
        if i == 1:
            for j in medium_input:
                if len(j) == 1:
                    singles = "(" + "'" + j[0] + "'" + ")"
                    cache[i].append(singles)
        else:
            for j in medium_input:
                if len(j) == i:
                    cache[i].append(str(j))
    for k in cache:
        tmp_lst = sorted(cache[k])
        new_lst = ",".join(tmp_lst)
        cache[k] = new_lst
    keys = sorted(list(cache.keys()))
    #stop = keys[-1]
    lst = []
    for item in keys:
        lst.append(cache[item])
    output_str = "\n\n".join(lst)
    return output_str



def check(total, frequent_items):
    info = defaultdict(int)
    for i in frequent_items:
        for single_basket in total:
            pass_lst = [j for j in i if j in single_basket]
            if set(pass_lst) == set(i):
                info[i] += 1
    final_result = [(i,info[i]) for i in info.keys()]
    return final_result

case = int(sys.argv[1])
support = int(sys.argv[2])
input_path = sys.argv[3]
output_path = sys.argv[4]

sc = SparkContext('local[*]', 'task1').getOrCreate()
sc.setLogLevel("ERROR")

time_start = time.time()
#rdd = sc.textFile(input_path).filter(lambda x: x != "user_id,business_id")
rdd = sc.textFile(input_path).filter(lambda x: x[0].isalpha() == False)

if case == 1:
    rdd_c1 = rdd.map(lambda x: (x.split(",")[0],[x.split(",")[1]]))
    rdd_q1 = rdd_c1.reduceByKey(add_fc).mapValues(lambda x: list(set(x)))
    
else:
    rdd_c2 = rdd.map(lambda x: (x.split(",")[1], [x.split(",")[0]]))
    rdd_q2 = rdd_c2.reduceByKey(add_fc).mapValues(lambda x: list(set(x)))
    

rdd_next = rdd_q1 if case == 1 else rdd_q2

### Pass_1: read small subsets, assign low threshold
length_rdd = rdd_next.count()
lst1 = rdd_next.map(lambda x: x[1]).glom().map(lambda x: implement_son(x,length_rdd,support)).flatMap(lambda x: x).collect()
cand = sorted(list(set(lst1)), key=lambda x: (len(x), x))

#cand = rdd_next.map(lambda item: item[1]).glom().map(lambda item: implement_son(item,length_rdd,support)).flatMap(lambda item: item).\
 #   distinct().sortBy(lambda item:(len(item),item)).collect()


### Pass_2: check candidates is frequent or not in the entire set
lst2 = rdd_next.map(lambda x: x[1]).glom().map(lambda x: check(x,cand)).flatMap(lambda x: x).reduceByKey(add_fc).filter(lambda x: x[1]>=support).map(lambda x: x[0]).collect()
freq = sorted(list(set(lst2)), key=lambda x: (len(x), x))

##freq = rdd_next.map(lambda item: item[1]).glom().map(lambda item: check_in_total(item,cand)).flatMap(lambda item: item).\
  #  reduceByKey(add_fc).filter(lambda item: item[1] >= support).map(lambda item: item[0]).distinct().sortBy(lambda item:(len(item),item)).collect()

with open(output_path, 'w') as task1_file:
    out = 'Candidates:\n' + output(cand) + '\n\n' + 'Frequent Itemsets:\n' + output(freq)
    task1_file.write(out)

end_time = time.time()
duration = round(end_time - time_start, 2)
print('Duration:', duration)

