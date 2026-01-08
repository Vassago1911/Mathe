import os
import subprocess

def try_gpu_install():
    # Prüfen, ob das Tool nvidia-smi existiert (Indiz für NVIDIA-Treiber)
    try:
        subprocess.check_output(['nvidia-smi'])
        from cudf.pandas import install
        install()
        print("GPU-Beschleunigung aktiv.")
    except (FileNotFoundError, subprocess.CalledProcessError):
        print("Keine NVIDIA GPU gefunden - nutze normalen CPU-Modus.")

try_gpu_install()

from contextlib import contextmanager
import duckdb
import numpy as np
import pandas as pd 
from pandasql import sqldf
import time 
timestamp = lambda: time.strftime("%Y-%m-%d %H:%M:%S")  

@contextmanager
def connect_database():
    path = 'z2sql.db'
    con = duckdb.connect(path, config={
        'access_mode': 'automatic'
    })
    try:
        yield con
    finally:
        con.close()

def generate_all_dags_of_node_count(V:int=7):
    import pandas as pd

    # we consider dags without loops, hence we 
    # will generate all such edge_lists and add (v,v) 
    # as trivial edges to denote the vertex set - 
    # guaranteeing the edge list is sufficient to uniquely
    # define the graph

    # start with the complete dag on V vertices
    # generate and order the edge_list, then count them
    # with 2^n positional meaning
    print(timestamp(), "--- Generating all Graphs with NodeCount = V =",V)
    E_KV = [ (i,j) for i in range(V) for j in range(V) if i<=j ]
    E_KV = sorted( E_KV, key = lambda x: ( 0,x[0],x[1] ) if x[1]==x[0] else (1,x[0],x[1]) )

    V_KV = list(filter(lambda x: x[0]==x[1], E_KV))
    e_KV = list(filter(lambda x: x[0]<x[1], E_KV))

    order = len(e_KV)
    index_set = range(2**order)

    res = dict()

    for ix in index_set:
        graph_id = f"{str(bin(ix))[2:]:0{order}}"[::-1]
        edges = []
        for i,yn in enumerate(graph_id):
            if yn:
                edges.append( e_KV[i] )
        edges = tuple(sorted(edges))
        res[ix] = { "name": graph_id, "edges": edges }

    graphs = [ (ix,res[ix]['name']) for ix in res.keys() ]
    graphs = pd.DataFrame(graphs, columns=['ix','bin_name'])
    graphs['name'] = 16*' ' # want to be able to save _one_ colloquialism per graph

    edges = [ (ix, i,j)  for ix in res.keys() for i,j in res[ix]['edges'] ]
    edges = pd.DataFrame(edges, columns=['ix','s_V','t_V'])   

    print(timestamp(), "--- Generated all Graphs with NodeCount = V =", V)
    return graphs, edges

# con = connect_database()
with connect_database() as conn:
    G,E = generate_all_dags_of_node_count(7) 
    print(G.head())
    print(E.head())   
