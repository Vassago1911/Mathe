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

def setup_database():
	con = duckdb.connect('z2sql.db', config={
	    'storage_compatibility_version': 'v0.10.2', # Manchmal hilft Abwärtskompatibilität
	    'access_mode': 'automatic'
	})
	return con
