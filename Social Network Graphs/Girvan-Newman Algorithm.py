import sys
import os
import time
from pyspark import SparkContext
from collections import defaultdict
from random import sample
import math
from queue import Queue
#### commandline: /usr/bin/python3 /Users/xiangyuanchi/Downloads/TestProgram/task2.py <filter threshold> <input_file_path> <betweenness_output_file_path> <community_output_file_path>
#### /usr/bin/python3 /Users/xiangyuanchi/Downloads/TestProgram/task2.py 7 /Users/xiangyuanchi/Downloads/TestProgram/ub_sample_data.csv /Users/xiangyuanchi/Downloads/TestProgram/btw.txt /Users/xiangyuanchi/Downloads/TestProgram/comm.txt

def add_fc(a,b):
    return a + b

def assign_score(input_root):
    score_dict = defaultdict(float)
    for v in vertices:
        if v != input_root:
            score_dict[v] = 1
    return score_dict


def find_all_neignbors(input_children, neighbors,visited, input_parent, path_dict,updated_nodes): 
    # parameters given: find_all_children(find_child, level_1_nodes, used_nodes, find_parent,num_path,new_nodes)
    for node in neighbors:
        adj_nodes = adj_nodes_dic[node]
        child_nodes = adj_nodes - visited
        input_children[node] = child_nodes
        for item in input_children:
            vals = input_children[item]
            for v in vals:
                input_parent[v].add(item)
        parent_nodes = input_parent[node]

        if len(parent_nodes) > 0:
            path_dict[node] = sum([path_dict[pn] for pn in parent_nodes])
        else:
            path_dict[node] = 1
        updated_nodes = updated_nodes.union(adj_nodes)
    find_parent = input_parent
    return (updated_nodes, find_parent,path_dict)


# parameters given: dfs_btw(root, level, tree, find_parent, num_path)
def dfs_btw(input_root, curr_level, tree,parent_dict, paths):
    parent_value = assign_score(input_root)
    edge_value = {}
    while curr_level != 1:
        for node in tree[curr_level-1]:
            parent_nodes = parent_dict[node]
            for parent_node in parent_nodes:
                w = paths[parent_node]/paths[node]
                edge_value[tuple(sorted((node, parent_node)))] = w * parent_value[node]
                parent_value[parent_node] += edge_value[tuple(sorted((node, parent_node)))]
        curr_level -= 1
    return edge_value


def GN(root):
    tree = dict()
    tree[0] = root

    num_path = dict()
    num_path[root] = 1
    
    used_nodes = {root}
    level_1_nodes = adj_nodes_dic[root]

    find_parent = defaultdict(set)
    find_child = {root: adj_nodes_dic[root]}
    level = 1

    while level_1_nodes != set():
        tree[level] = level_1_nodes
        used_nodes = used_nodes.union(level_1_nodes)
        new_nodes = set()

        
        new_nodes = find_all_neignbors(find_child, level_1_nodes, used_nodes, find_parent,num_path,new_nodes)[0]
        find_parent = find_all_neignbors(find_child, level_1_nodes, used_nodes, find_parent,num_path,new_nodes)[1]
        num_path = find_all_neignbors(find_child, level_1_nodes, used_nodes, find_parent,num_path,new_nodes)[2]

        level_nodes = new_nodes - used_nodes
        level_1_nodes = level_nodes
        level += 1

	# calculate betweenness from bottom 
    bottom_up_edges = dfs_btw(root, level, tree, find_parent, num_path)

    return [(k, v) for k, v in bottom_up_edges.items()]



def find_community(input_n, input_graph, visited):
    community = [input_n]
    visited.add(input_n)
    next_queue = [input_n]
    while next_queue:
        node = next_queue.pop(0)
        for child in input_graph[node]:
            if child not in visited:
                next_queue.append(child)
                community.append(child)
                visited.add(child)
    return community

def find_communities(input_graph):
    visited = set()
    communities = []
    for node in input_graph:
        if node not in visited:
            community = find_community(node, input_graph, visited)
            communities.append(community)
    return communities



def calculate_modularity(modules, input_degree_matrix, input_edges, m):
    modularity = 0
    for module in modules:
        modularity += calculate_q(module, input_degree_matrix, input_edges, m)
    return modularity / (2 * m)


def calculate_q(input_module, input_degree_matrix, input_edges, m):
    res = 0
    for i in input_module:
        for j in input_module:
            pair = (i,j)
            A_i_j = 1 if pair in input_edges else 0
            res += (A_i_j - ((input_degree_matrix[i] * input_degree_matrix[j]) / (2 * m)))
    return res


def output_file(file_path, data):
    with open(file_path, "w+") as f:
        for i in data:
            f.write(str(i)[1:-1] + "\n")


threshold = sys.argv[1]
input_file = sys.argv[2]
btw_path = sys.argv[3]
comm_path = sys.argv[4]

sc = SparkContext("local[*]", "task2").getOrCreate()
sc.setLogLevel("WARN")
time_start = time.time()
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
                    v.add((i[0]))
                    v.add((j[0]))
                    e.add((i[0],j[0]))
    return (v,e)

vertices = graph_design(rdd.collect())[0]
edges = graph_design(rdd.collect())[1]

# Establish a neighborhood {node: [adj_nodes]}
adj_nodes_dic = defaultdict(list)
for item in edges:
    adj_nodes_dic[item[0]].append(item[1])
for i in adj_nodes_dic:
    adj_nodes_dic[i] = set(adj_nodes_dic[i])


betweenness = sc.parallelize(vertices).map(lambda node: GN(node)).flatMap(lambda x: [pair for pair in x]).reduceByKey(add_fc).map(lambda x: (x[0], x[1] / 2)).collect()
betweenness = sorted(list(betweenness), key = lambda item: (-item[1], item[0]))

### Task2_1: Betweenness Calculation
btw_output = output_file(btw_path, betweenness)


##################################################################
# Create Adjacency Matrix and Degree Matrix
def matrix_table(neignbor_dict, input_vertices, input_edges):
    degree = {k: len(v) for k,v in neignbor_dict.items()}
    adjacency = {}
    for i in vertices:
        for j in vertices:
            pair = (i,j)
            adjacency[pair] = 1 if pair in input_edges else 0
            #adjacency[(i, j)] = 1 if (i,j) in input_edges else 0
    return (degree, adjacency)

degree_matrix = matrix_table(adj_nodes_dic, vertices, edges)[0]
adjacency_matrix = matrix_table(adj_nodes_dic, vertices, edges)[1]
m = len(edges) / 2
left_edges = m
max_mod = -math.inf

            
while left_edges != 0:
    highest_betweenness = betweenness[0][1]
    for pair in betweenness:
        if pair[1] == highest_betweenness:
            adj_nodes_dic[pair[0][0]].remove(pair[0][1])
            adj_nodes_dic[pair[0][1]].remove(pair[0][0])
            left_edges -= 1
    #left_edges = cut_edge(betweenness, highest_betweenness, adj_nodes_dic, left_edges)[0]
    #adj_nodes_dic = cut_edge(betweenness, highest_betweenness, adj_nodes_dic, left_edges)[1]
    temp_communities = find_communities(adj_nodes_dic)
    #temp_communities = find_communities(sample(vertices, 1)[0], vertices, adj_nodes_dic)
    #print(temp_communities)
    cur_modularity = calculate_modularity(temp_communities, degree_matrix, edges, m)
    if cur_modularity > max_mod:
        max_mod = cur_modularity
        communities = temp_communities
    if left_edges == 0:
        break

    betweenness = sc.parallelize(vertices).map(lambda node: GN(node)).flatMap(lambda x: [pair for pair in x]).reduceByKey(add_fc).map(lambda x: (x[0], x[1] / 2)).collect()
    betweenness = sorted(list(betweenness), key = lambda item: (-item[1], item[0]))


### Task2_2: Community Detection 
sorted_communities = sc.parallelize(communities).map(lambda x: sorted(x)).sortBy(lambda x: (len(x), x)).collect()
comm_output = output_file(comm_path, sorted_communities)

# Reference:
# https://github.com/hsuanhauliu/girvan-newman-community-detection/blob/master/community_detection/gn.py