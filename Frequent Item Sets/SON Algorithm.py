import sys
from pyspark import SparkContext
import os
import time
from itertools import combinations
import math
import csv
from collections import defaultdict

### command line 
### /usr/bin/python3 /Users/xiangyuanchi/Downloads/TestProgram/new_task2.py 20 50  /Users/xiangyuanchi/Downloads/TestProgram/ta_feng_all_months_merged.csv /Users/xiangyuanchi/desktop/tfa

def add_fc(a,b):
    return a + b 



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


def apriori(basket, threshold, target):
    while target == 1:
        return single(basket, threshold)
    prev_freq = single(basket,threshold)
    cache = [(i,) for i in prev_freq]
    flag = target
    while flag > 1:
        info = {}
        final_candidate = []
        cnt = target-flag+2
        
        if cnt == 2:
            stage_freq_single = prev_freq
        else:
            stage_freq_single = set()
            for combo in prev_freq:
                for item in combo:
                    stage_freq_single.add(item)
            stage_freq_single = sorted(stage_freq_single)
        
        for b in basket:
            bas = sorted(set(b).intersection(set(stage_freq_single)))
            pool = combinations(bas, cnt)
            for i in pool:
                item = tuple(i)
                info[item] = info.get(item, 0) + 1
        
        
        for candidate in info.keys():
            if info[candidate] >= threshold:
                final_candidate.append(candidate)
        final_candidate = [tuple(sorted(i)) for i in final_candidate]
        final_candidate = list(set(final_candidate))
        cache = cache + final_candidate
        if final_candidate == []:
            break
        
        flag -= 1
        prev_freq = final_candidate
    return cache

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
    lst = []
    for item in keys:
        lst.append(cache[item])
    output_str = "\n\n".join(lst)
    return output_str


def check(total_market_basket, frequent_items):
    info = defaultdict(int)
    for i in frequent_items:
        for single_basket in total_market_basket:
            pass_lst = [j for j in i if j in single_basket]
            if set(pass_lst) == set(i):
                info[i] += 1
    final_result = [(i,info[i]) for i in info.keys()]
    return final_result


filter_threshold = int(sys.argv[1])
support = int(sys.argv[2])
input_path = sys.argv[3]
output_path = sys.argv[4]

### RDD Data Preprocessing
sc = SparkContext('local[*]', 'task2').getOrCreate()
sc.setLogLevel("ERROR")
rdd_tfa = sc.textFile(input_path)
header = rdd_tfa.first()
rdd_new = rdd_tfa.filter(lambda item: item != header)
rdd_new = rdd_new.map(lambda row: row.split(',')).map(lambda r: (r[0].strip('"') + "-"+ r[1].strip('"'), str(int(r[5].strip('"'))))).collect()

with open("customer_product.csv", "w") as file:
    file.write("DATE-CUSTOMER_ID,PRODUCT_ID\n")
    for line in rdd_new:
        fmt_line = str(line[0]) + "," + str(line[1]) + '\n'
        file.write(fmt_line)
        
        
### Apply SON Algorithm
start = time.time()
rdd_cp = sc.textFile("customer_product.csv")
title = rdd_cp.first()
rdd_son = rdd_cp.filter(lambda item: item != title).map(lambda item:(item.split(",")[0],[item.split(",")[1]]))
rdd_final = rdd_son.reduceByKey(add_fc).mapValues(lambda item: list(set(item))).filter(lambda item: len(item[1]) > filter_threshold)
length_rdd = rdd_final.count()


lst1 = rdd_final.map(lambda x: x[1]).glom().map(lambda x: implement_son(x,length_rdd,support)).flatMap(lambda x: x).collect()
res1 = sorted(list(set(lst1)), key=lambda x: (len(x), x))

lst2 = rdd_final.map(lambda x: x[1]).glom().map(lambda x: check(x,res1)).flatMap(lambda x: x).reduceByKey(add_fc).filter(lambda x: x[1]>=support).map(lambda x: x[0]).collect()
res2 = sorted(list(set(lst2)), key=lambda x: (len(x), x))
     
with open(output_path, "w") as ff:
    res_file = "Candidates:\n" + output(res1) + "\n\n" + "Frequent Itemsets:\n" + output(res2)
    ff.write(res_file)    

end = time.time()
timeslot = round(end - start, 2)
print("Duration:", timeslot)
    
