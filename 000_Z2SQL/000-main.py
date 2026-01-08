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

import duckdb
import pandas as pd 
from contextlib import contextmanager

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
    # we consider dags without loops, hence we 
    # will generate all such edge_lists and add (v,v) 
    # as trivial edges to denote the vertex set - 
    # guaranteeing the edge list is sufficient to uniquely
    # define the graph

    # start with the complete dag on V vertices
    # generate and order the edge_list, then count them
    # with 2^n positional meaning
    E_KV = [ (i,j) for i in range(V) for j in range(V) if i<=j ]
    E_KV = sorted( E_KV, key = lambda x: ( 0,x[0],x[1] ) if x[1]==x[0] else (1,x[0],x[1]) )

    V_KV = list(filter(lambda x: x[0]==x[1], E_KV))
    e_KV = list(filter(lambda x: x[0]<x[1], E_KV))

    order = len(e_KV)
    index_set = range(2**order)

    # TODO: count now
    return E_KV, V_KV, e_KV, order, index_set

# con = connect_database()
with connect_database() as conn:
    t = generate_all_dags_of_node_count(7)    
    print(t)