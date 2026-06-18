#!/share/pkg.8/python3/3.12.4/install/bin/python3
import warnings
warnings.filterwarnings('ignore')

# Read the node data file
# Get a list of unique cpu_arch names and a representative
# hostname for each
# ssh to each, run lscpu, parse the output for the presence
# of AVX2 and AVX512 instructions. These correspond to our
# qsub flags "-l avx2" and "-l avx512"
#
# write out a file: cpu_arch_simd.csv
# file:
#   cpu_arch,avx,avx2
#   broadwell,yes,no
#   graniterapids,yes,yes
#   ...etc...

import subprocess
import joblib
import pandas as pd
import shlex
import numpy as np


def lscpu(row):
    ''' ssh to a host, call lscpu, return a tuple:
            cpu_arch,avx,avx2
        like:   graniterapids,yes,yes '''
    cpu_arch, hostname = row
    # to ssh without being asked about host identity. This won't
    # update your known_hosts file
    #  ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
    cmd = f'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {hostname} lscpu'
    cmd = shlex.split(cmd)
    
    try:
        popen = subprocess.run(cmd, capture_output=True)
        stdout = popen.stdout.decode('utf-8')
        avx2 = 'yes' if stdout.find('avx2') >= 0 else 'no'
        avx512 = 'yes' if stdout.find('avx512') >= 0 else 'no'
        return cpu_arch,avx2,avx512
    except:
        # That host is down...
        return cpu_arch,None,None

if __name__=='__main__':
    df = pd.read_csv("/projectnb/rcsmetrics/nodes/data/nodes.csv")
    # Group by cpu_arch, randomly grab 3 hosts. reset_index() makes it
    # a dataframe of cpu_arch and hostname. All three hosts will be
    # queried, just in case one of them is not available.
    random_hosts = df.groupby('cpu_arch')['host'].apply(lambda s: s.sample(n=3, replace=True).iloc[0]).reset_index()
    # here we get fancy and run all the lscpu() calls in parallel
    chunks = random_hosts.values.tolist()
    arch_info = joblib.Parallel(n_jobs=-2)(joblib.delayed(lscpu)(row) for row in chunks)
    # Concatenate the pieces back into a dataframe
    df2 = pd.DataFrame(arch_info,columns=['cpu_arch','avx2','avx512'])
    # Remove any rows with None values.
    df2 = df2.dropna()
    # Drop duplicates
    df2 = df2.drop_duplicates().sort_values(by='cpu_arch')
    # And write it out
    df2.to_csv('cpu_arch_simd.csv', index=False)
    
