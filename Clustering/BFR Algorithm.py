import sys
import os
import time
import random
import math
import numpy as np
from sklearn.cluster import KMeans
from pyspark import SparkContext

def implement_RS(rs_dict):
    RS = []
    for k,v in rs_dict.items():
        if len(v) == 1:
            RS.append(rs_dict[k][0])
    return RS

        
# def implement_CS(data, CS, cs_centroids, cs_id_cluster):
#     for c in data.keys():
#         if len(data[c]) > 1:
#             N = len(data[c])
#             pure_points = []
#             tmp_list = []
#             for i in range(N):
#                 pure_points.append(data[c][i][1])
#                 tmp_list.append(data[c][i][0])
#             cs_id_cluster.append(tmp_list)
#             pts_vec = np.array(pure_points)
#             SUM = pts_vec.sum(axis=0)
#             pts_square_vec = pts_vec ** 2
#             SUMSQ = pts_square_vec.sum(axis=0)
#             CS.append(np.array([N,SUM,SUMSQ],dtype=object))
#             center = SUM/N
#             cs_centroids.append(center)


def implement_CS(data, CS, cs_centroids, cs_id_cluster):
    for cluster in data.values():
        if len(cluster) > 1:
            points = np.array([p[1] for p in cluster])
            ids = [p[0] for p in cluster]
            cs_id_cluster.append(ids)
            n_points = points.shape[0]
            sum_points = points.sum(axis=0)
            sumsq_points = (points ** 2).sum(axis=0)
            CS.append(np.array([n_points, sum_points, sumsq_points], dtype=object))
            center = sum_points / n_points
            cs_centroids.append(center)
            
def calculate_MD(x,centroid,stat):
    N, SUM, SUMSQ = stat[0], stat[1], stat[2]
    variance = (SUMSQ/N) - pow((SUM / N),2)
    tmp_sum = 0
    for i in range(len(x)):
        if variance[i] == 0:
            return math.inf
        tmp_sum += pow((x[i] - centroid[i]), 2)/ variance[i]
    res = math.sqrt(tmp_sum)
    return res

def Mah_dist(x,ds_centroids, DS):
   Mahalanobis_Distance = []
   for centroid, stat in zip(ds_centroids, DS):
       M_Dis = calculate_MD(x,centroid,stat)
       Mahalanobis_Distance.append(M_Dis)
   return Mahalanobis_Distance

def assign_to_DS(data_list, ds_centroids, DS, cluster_id):
    new_data_list = []
    for point in data_list:
        x = np.array(point[1])
        Mahalanobis_Distances = Mah_dist(x,ds_centroids, DS)
        threshold = min(Mahalanobis_Distances)
        if threshold < 2 * math.sqrt(len(x)):
            close_cls = np.argmin(Mahalanobis_Distances)
            DS[close_cls][0] += pow(x, 0)
            DS[close_cls][1] += pow(x, 1)
            DS[close_cls][2] += pow(x, 2)
            ds_centroids[close_cls] = DS[close_cls][1] / DS[close_cls][0]
            cluster_id[close_cls].append(point[0])
        else:
            new_data_list.append(point)
    return new_data_list

    
def merge_CS(CS, cs_centroids, cs_id_cluster):
    new_CS, new_centroids, new_id_cluster = [], [], []
    while len(CS) > 0:
        c, center, ids = CS[0], cs_centroids[0], cs_id_cluster[0]
        CS, cs_centroids, cs_id_cluster = CS[1:], cs_centroids[1:], cs_id_cluster[1:]
        merged = False
        for i, (stat, centroid, pts) in enumerate(zip(CS, cs_centroids, cs_id_cluster)):
            if min(calculate_MD(center, centroid, stat), calculate_MD(centroid, center, c)) < 2 * math.sqrt(len(centroid)):
                merged = True
                c = c + stat
                center = (center + centroid) / 2
                ids.extend(pts)
                CS.pop(i), cs_centroids.pop(i), cs_id_cluster.pop(i)
                break
        if not merged:
            new_CS.append(c), new_centroids.append(center), new_id_cluster.append(ids)
    return new_CS, new_centroids, new_id_cluster

def num_count(S):
    total = 0
    for item in S:
        cnt = item[0]
        total += cnt
    return total


##### RDD
input_file = sys.argv[1]
n_cluster = int(sys.argv[2])
output_file = sys.argv[3]

def str2num(value): # value:dict
    tmp = []
    for i in range(len(value)):
        tmp.append(float(value[i]))
    return tmp

def f_list(x):
    return list(x)

sc = SparkContext("local[*]", "task").getOrCreate()
sc.setLogLevel("WARN")
time_start = time.time()

text_rdd = sc.textFile(input_file).map(lambda line : line.split(",")).map(lambda lst : (lst[0],lst[2:])).map(lambda x: (int(x[0]), str2num(x[1]))).collect()
data = random.sample(text_rdd, len(text_rdd))
size = math.ceil(len(data)/5)
rounds_data = []
for i in range(5):
    rounds_data.append(text_rdd[size*i:size*(i+1)])

    
##### Step 1-5
init_data = rounds_data[0]
pure_points = sc.parallelize(init_data).map(lambda x:x[1]).collect()
kmeans = KMeans(n_clusters = 25 * n_cluster)
X = np.array(pure_points)
y_kmeans= kmeans.fit_predict(X)
rs_data = sc.parallelize(zip(y_kmeans, init_data)).groupByKey().mapValues(f_list).collect()
rs_data_dict = {}
for i in range(len(rs_data)):
    rs_data_dict[rs_data[i][0]] = rs_data[i][1]
RS = implement_RS(rs_data_dict)
for id_pt in RS:
    init_data.remove(id_pt)

# Run K-Means again
kmeans = KMeans(n_clusters = n_cluster)
pure_points = sc.parallelize(init_data).map(lambda pair : pair[1]).collect()
X = np.array(pure_points)
y_kmeans = kmeans.fit_predict(X)
ds_data_lst = sc.parallelize(zip(y_kmeans, init_data)).groupByKey().mapValues(f_list).collect()
ds_data_lst = sorted(ds_data_lst, key = lambda x: x[0])
ds_data = {}
for i in range(len(ds_data_lst)):
    ds_data[ds_data_lst[i][0]] = ds_data_lst[i][1]
DS, ds_centroids, cluster_id = [], [], []
for k,v in ds_data.items():
    N = len(ds_data[k])
    pure_points, lst = [], []
    for i in range(N):
        pure_points.append(v[i][1])
        lst.append(v[i][0])
    cluster_id.append(lst)
    pts_vec = np.array(pure_points)
    SUM = pts_vec.sum(axis=0)
    pts_square_vec = pow(pts_vec, 2)
    SUMSQ = pts_square_vec.sum(axis=0)
    parameters = [N,SUM,SUMSQ]
    DS.append(np.array(parameters,dtype = object))
    ds_centroids.append(SUM / N)

##### Step 6
kmeans = KMeans(n_clusters = n_cluster)
pure_points = sc.parallelize(RS).map(lambda pair : pair[1]).collect()
X = np.array(pure_points)
y_kmeans = kmeans.fit_predict(X)
tmp_data_lst = sc.parallelize(zip(y_kmeans, RS)).groupByKey().mapValues(f_list).collect()
tmp_data = {}
for i in range(len(tmp_data_lst)):
    tmp_data[tmp_data_lst[i][0]] = tmp_data_lst[i][1]
RS = implement_RS(tmp_data)
CS = []
cs_centroids = []
cs_id_cluster = []
implement_CS(tmp_data, CS, cs_centroids, cs_id_cluster)
intermediate_list = []
intermediate_list.append([num_count(DS), len(CS), num_count(CS), len(RS)])

##### Step 7-12: repeat
def load_chunks(input_chunks):
    global ds_centroids
    global DS
    global cluster_id
    global RS
    global CS
    global cs_centroids
    global cs_id_cluster
    global intermediate_list
    
    n = 1
    while (n < len(rounds_data)):
        cur_round_data = input_chunks[n]
        cur_round_data = assign_to_DS(cur_round_data, ds_centroids, DS, cluster_id)
        
        #assign_to_RS(cur_round_data, RS)
        for p in cur_round_data:
            RS.append(p)
        kmeans = KMeans(n_clusters = 3 * n_cluster)
        pure_points = sc.parallelize(RS).map(lambda x: x[1]).collect()
        X = np.array(pure_points)
        y_kmeans = kmeans.fit_predict(X)
        tmp_data_lst = sc.parallelize(zip(y_kmeans, RS)).groupByKey().mapValues(f_list).collect()
        tmp_data = {}
        for i in range(len(tmp_data_lst)):
            tmp_data[tmp_data_lst[i][0]] = tmp_data_lst[i][1]
        RS = implement_RS(tmp_data)
        implement_CS(tmp_data, CS, cs_centroids, cs_id_cluster)
        CS, cs_centroids, cs_id_cluster = merge_CS(CS, cs_centroids, cs_id_cluster)
        n += 1
        if n == 5:
            final_CS, final_cs_centriods, final_cs_id_cluster = [], [], []
            for i, j, k in zip(CS, cs_centroids, cs_id_cluster):
                flag = False
                for u, v, w in zip(DS, ds_centroids, cluster_id):
                    d1 = calculate_MD(j,v,u)
                    d2 = calculate_MD(v,j,i)
                    MD = min(d1, d2)
                    if MD < 2 * math.sqrt(len(v)):
                        flag = True
                        new_ds_lst = i + u
                        new_ds_center = (j+v)/2
                        c_id = cluster_id.index(w)
                        DS[c_id] = new_ds_lst
                        ds_centroids[c_id] = new_ds_center
                        for p in k:
                            w.append(p)
                        break
                if not flag:
                    final_CS.append(i)
                    final_cs_centriods.append(j)
                    final_cs_id_cluster.append(k)
            CS = final_CS
            cs_centroids = final_cs_centriods
            cs_id_cluster = final_cs_id_cluster

        intermediate_list.append([num_count(DS), len(CS), num_count(CS), len(RS)])
load_chunks(rounds_data)




##### Export file
def cluster_res(input_id, cs_id_cls,input_rs): # (cluster_id, cs_id_cluster,RS)
    ans = []
    for i in range(len(cluster_id)):
        for j in cluster_id[i]:
            ans.append((j,i))
    for cls in cs_id_cls:
        for i in cls:
            ans.append((i,-1))
    for rs_pnt in RS:
        pnt = rs_pnt[0]
        ans.append((pnt,-1))
    ans.sort(key = lambda x:x[0])
    return ans
res = cluster_res(cluster_id, cs_id_cluster, RS)


def intermediate_fc(round_num):
    line = ""
    for num in intermediate_list[round_num]:
        if type(num) == int:
            line += str(num) + ","
        else:
            line += str(int(num[0])) + ","
    tmp_line = "Round " + str(round_num+1) + ": " + line[:-1]
    return tmp_line + "\n"

with open(output_file, "w") as f:
    f.write("The intermediate results:\n")
    for i in range(5):
        new_line = intermediate_fc(i)
        f.write(new_line)
    
    f.write("\nThe clustering results:\n")
    for val in res:
        f.write(str(val[0])+","+str(val[1])+"\n")

    f.close()

time_end = time.time()
duration = round(time_end - time_start, 2)
print('Duration:', duration)





