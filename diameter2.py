# -*- coding: utf-8 -*-
#! /usr/bin/python3

import time
import math
import copy
import multiprocessing
import pandas as pd



def make_graph(df1,df2):
    
    articles_name = df1.name
    Graph = {}
    node = {}
    
    i = 0     
    for name in articles_name:
        node['neighbors'] = df2.loc[i].out_links
        node['dist'] = math.inf
        node['processed'] = 0 #not processed
        Graph[name] = node
        node = {}
        i += 1
                
    return Graph
    
#endOfFunction*****************************************************************

def bfs(start,graph):
    # calculate shorest path between start and all other nodes in graph    
    current = [start]
    graph[start]['processed'] = 1
    tmp = []
    depth = 0

    if len(graph[start]['neighbors']) == 0:
        return 0 # start does not have any out links
    
    while True:
        if len(current) == 0:
            return depth-1 
            
        for node in current:
            for child in graph[node]['neighbors']:
                if child in graph and graph[child]['processed'] == 0:
                     tmp.append(child)
                     graph[child]['processed'] = 1            
        depth += 1
        current = tmp
        tmp = []
            
    
       
#endOfFunction*****************************************************************    
    
def diameter(graph):        
    lengths = []
    counter = 0
    i = 0
    for node in graph.keys():
        g = copy.deepcopy(graph)
        re = bfs(node,g)
        lengths.append(re)
        counter += 1
        i += 1
        if i == 30:
            print(str(counter)+" nodes were processes...")
            i = 0
        
    return max(lengths)
            
    
#endOfFinction*****************************************************************

#start

start = time.time()

store = pd.HDFStore('store2.h5')
df1=store['df1']
df2=store['df2']


graph = make_graph(df1,df2)
d = diameter(graph)
print("The diameter is : ",str(d))

store.close()
print("Runtime : ",str(time.time()-start)," sec")


