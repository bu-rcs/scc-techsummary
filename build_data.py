#!/share/pkg.8/python3/3.12.4/install/bin/python3

import pandas as pd
import json
import sys, os

# set output filepath/name
output_filename = sys.argv[1] if len(sys.argv) > 1 else 'data.js'

# set git/data dir
git_dir = os.path.dirname(os.path.abspath(__file__))
extrainfo_f = os.path.join(git_dir, "extrainfo.csv")
cpulinks_f  = os.path.join(git_dir, "cpulinks.csv")
gpulinks_f  = os.path.join(git_dir, "gpulinks.csv")
simd_f      = os.path.join(git_dir, "cpu_arch_simd.csv")
NODES_FILE = "/projectnb/rcsmetrics/nodes/data/nodes.csv"

# validate file existence
for file_path in [extrainfo_f, cpulinks_f, gpulinks_f, simd_f, NODES_FILE]:
    if not os.path.exists(file_path):
        print(f"The file '{file_path}' does not exist!")
        exit(1)

# load nodes data file
df = pd.read_csv(NODES_FILE)
extra_notes = pd.read_csv(extrainfo_f)

# merge extra nodes onto base dataframe
df = pd.merge(df, extra_notes, on='host', how='left')
df['notes'] = df['notes'].fillna('None')

# keep only active nodes
df = df[df["netbox_status"] == "Active"]

# Load the SIMD file
simd = pd.read_csv(simd_f)
# Change the columns...avx2,avx512 --> simd
#                       yes, yes   --> AVX2,AVX512
simd['simd'] = simd.apply(
    lambda r: ', '.join(
        label for label, col in [('AVX2', 'avx2'), ('AVX512', 'avx512')]
        if r[col] == 'yes'), axis=1,)
simd = simd.drop(columns=['avx2','avx512'])

# uncomment to help figure out cpu manual URL map
# print(df['processor_type'].unique())

# Clean up na values, reformat strings for output
df['gpu_type'] = df['gpu_type'].fillna('None')

# IMPORTANT: Save the original numeric gpu_cc BEFORE converting to text
df['gpu_cc_numeric'] = df['gpu_cc'].fillna(0)  # Keep numeric, default to 0
df['gpu_cc'] = df['gpu_cc'].fillna('None')
df['gpu_cc_text'] = df['gpu_cc'].apply(lambda x: f"Cuda GPU Compute Capability: {x}" if x != 'None' else x)

df['gpu_mem'] = df['gpu_mem'].fillna('None')
df['gpu_mem'] = df['gpu_mem'].apply(lambda x: f"GPU Memory: {x}GB" if x != 'None' else x)


# group by - ADD gpu_cc_numeric to grouping columns
group_cols = [
    'processor_type', 'cores', 'memory', 'scratch', 'eth_speed', 'gpu_type', 'gpus', 'flag', 'cpu_arch', 'gpu_cc', 'gpu_cc_text', 'gpu_mem', 'gpu_cc_numeric', 'ib_speed',
]

grouped = (
    df
    .groupby(group_cols)
    .agg(
        quantity=('host', 'count'),
        hostnames=('host', lambda x: sorted(list(x))), # optional: collect sorted list of hostnames per group,
        notes=('notes', lambda x: sorted(list(x))),
    )
    .reset_index()
)


# Sanity check on file
if len(grouped) < 100 and not 'scc1' in df['host'].values:
    print("File failed sanity check! Data file not renewed.")
    # TODO: send email
    exit(1)

# map cpu names from the file to anchor tags with links for better display
def cpulinks_to_href_dict(filepath):
    # Read the CSV into a DataFrame
    df = pd.read_csv(filepath, usecols=['cpu_model', 'cpu_display_name', 'cpu_url'])
    # Construct the href anchor based on cpu_url and cpu_display_name
    href =  "<a href=\"" + df['cpu_url'].astype(str) + "\" target=\"_blank\">" + df['cpu_display_name'].astype(str) + "</a>"
    # Create a dictionary with 'cpu_model' as keys and the href anchor string as values
    result = pd.Series(href.values, index=df['cpu_model']).to_dict()
    
    return result

def gpulinks_to_href_dict(filepath):
    df = pd.read_csv(filepath, usecols=['gpu_model', 'gpu_url'])
    href =  "<a href=\"" + df['gpu_url'].astype(str) + "\" target=\"_blank\">" + df['gpu_model'].astype(str) + "</a>"
    result = pd.Series(href.values, index=df['gpu_model']).to_dict()
    
    return result

cpu_display_map = cpulinks_to_href_dict(cpulinks_f)
gpu_display_map = gpulinks_to_href_dict(gpulinks_f)

# check if all CPUs and GPUs have corresponding links
dif = set(grouped['processor_type']) - set(cpu_display_map.keys())
dif.discard('None')
if len(dif):
    print(f"Missing CPU links for: ", dif)
    # exit(1)

dif = set(grouped['gpu_type']) - set(gpu_display_map.keys())
dif.discard('None')
if len(dif):
    print(f"Missing GPU links for: ", dif)
    # exit(1)

grouped['processor_type'] = grouped['processor_type'].map(cpu_display_map)
grouped['gpu_type'] = grouped['gpu_type'].map(gpu_display_map)
grouped['gpu_type'] = grouped['gpu_type'].fillna('None')

# add architecture type
grouped['processor_type'] = grouped['processor_type'] + "<br>"
grouped['processor_type'] = grouped['processor_type'] + grouped["cpu_arch"]
grouped['extra_info'] = grouped.apply(
    lambda r: [r['gpu_cc_text'], r['gpu_mem'], *r['notes']], axis=1
)

grouped['extra_info'] = grouped['extra_info'].apply(lambda x: [v for v in x if v != "None"])
grouped['flag'] = grouped['flag'].map({'S':'Shared', 'B':'Buy In'})

# Add the SIMD info columns avx2 and avx512
grouped = grouped.merge(simd, on='cpu_arch', how='left')

# make sure default ordering by hostname
grouped = grouped.sort_values(
    by="hostnames",
    key=lambda col: col.map(
        lambda hosts: (
            hosts[0],          # primary: lexicographic
            len(hosts[0])      # secondary: shorter first
        ) if isinstance(hosts, list) and hosts else ("", 0)
    )
)

# Save in JS display order: [hostnames, processor_type, cores, memory, gpu_type, gpus, flag, extra_info, gpu_cc_numeric]
export_data = grouped.apply(
    lambda row: [
        row['hostnames'], 
        row['processor_type'], 
        row['cores'], 
        row['memory'], 
        row['gpu_type'], 
        row['gpus'], 
        row['flag'], 
        row['extra_info'],
        float(row['gpu_cc_numeric']),  # Export as number for filtering
        row['ib_speed'],
        row['eth_speed'],
        row['simd']
    ],
    axis=1
).tolist()

# output to a "js" file, containing just the const array that will be used for the table
with open(output_filename, 'w') as outfile:
    outfile.write("const data = ")
    json.dump(export_data, outfile, indent=2)
    outfile.write(";")
