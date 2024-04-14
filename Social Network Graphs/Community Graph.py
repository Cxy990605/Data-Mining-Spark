import sys
import os
import time
from pyspark import SparkContext
from pyspark.sql import SparkSession,SQLContext
from graphframes import GraphFrame
#os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell"

#### commandline: /usr/bin/python3 /Users/xiangyuanchi/Downloads/TestProgram/task1.py <filter threshold> <input_file_path> <community_output_file_path>
#### /usr/bin/python3 /Users/xiangyuanchi/Downloads/TestProgram/task1.py 7 /Users/xiangyuanchi/Downloads/TestProgram/ub_sample_data.csv /Users/xiangyuanchi/Downloads/TestProgram/output.txt
threshold = sys.argv[1]
input_file = sys.argv[2]
output_file = sys.argv[3]

sc = SparkContext("local[*]", "task1").getOrCreate()
sc.setLogLevel("WARN")
time_start = time.time()
#rdd = sc.textFile(input_file).filter(lambda x: x!= "user_id,business_id").map(lambda x: (x.split(",")[0], x.split(",")[1]))
rdd = sc.textFile(input_file).filter(lambda x: x!= "user_id,business_id").map(lambda x: x.split(",")).map(lambda x: (x[0], x[1])).groupByKey().map(lambda x: (x[0], sorted(list(set(x[1])))))

def inter(lst1, lst2):
    return list(set(lst1) & set(lst2))

def graph_design(input_data):
    v, e = set(), set()
    for i in input_data:
        for j in input_data:
            if i[0] == j[0]:
                continue
            else:
                length = len(inter(i[1], j[1]))
                if length >= int(threshold):
                    v.add((i[0],))
                    v.add((j[0],))
                    e.add((i[0],j[0]))
    return (v,e)

vertices = list(graph_design(rdd.collect())[0])
edges = list(graph_design(rdd.collect())[1])


### Implement LPA method
sql_df = SQLContext(sc)
vertices_df = sql_df.createDataFrame(vertices, ["id"])
edges_df = sql_df.createDataFrame(edges, ["src", "dst"])
g = GraphFrame(vertices_df, edges_df)
community = g.labelPropagation(maxIter = 5)

# Convert dataframe to rdd
#print(type(community))
output_comm = community.rdd
output_comm = output_comm.map(lambda x:(x[1], x[0])).groupByKey().map(lambda x: sorted(list(x[1]))).sortBy(lambda x: (len(x), x))

with open(output_file, "w+") as f:
    for item in output_comm.collect():
        str_line = str(item).strip("[]")
        f.writelines(str_line + "\n")


time_end = time.time()
duration = round(time_end - time_start, 2)
print('Duration:', duration)

# Reference:
# https://graphframes.github.io/graphframes/docs/_site/user-guide.html
# https://sparkbyexamples.com/pyspark/pyspark-convert-dataframe-to-rdd/
# with open(file_path, 'w+') as output_file:
#         for id_array in result_array:
#             output_file.writelines(str(id_array)[1:-1] + "\n")
#         output_file.close()
