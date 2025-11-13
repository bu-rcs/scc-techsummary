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

# load nodes data file
df = pd.read_csv("/projectnb/rcsmetrics/nodes/data/nodes.csv")
extra_notes = pd.read_csv(extrainfo_f)

# merge extra nodes onto base dataframe
df = pd.merge(df, extra_notes, on='host', how='left')
df['notes'] = df['notes'].fillna('None')

# keep only active nodes
df = df[df["netbox_status"] == "Active"]

# uncomment to help figure out cpu manual URL map
# print(df['processor_type'].unique())

# Clean up na values, reformat strings for output
df['gpu_type'] = df['gpu_type'].fillna('None')
df['gpu_cc'] = df['gpu_cc'].fillna('None')
df['gpu_cc'] = df['gpu_cc'].apply(lambda x: f"Cuda GPU Compute Capability: {x}" if x != 'None' else x)
df['gpu_mem'] = df['gpu_mem'].fillna('None')
df['gpu_mem'] = df['gpu_mem'].apply(lambda x: f"GPU Memory: {x}GB" if x != 'None' else x)

# group by:
group_cols = [
    'processor_type', 'cores', 'memory', 'scratch', 'eth_speed', 'gpu_type', 'gpus', 'flag', 'cpu_arch', 'gpu_cc', 'gpu_mem'
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
if len(grouped) < 100 and not 'scc1' in df['host']:
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

cpu_display_map = cpulinks_to_href_dict(cpulinks_f)


grouped['processor_type'] = grouped['processor_type'].map(cpu_display_map)

# add architecture type
grouped['processor_type'] = grouped['processor_type'] + "<br>"
grouped['processor_type'] = grouped['processor_type'] + grouped["cpu_arch"]
grouped['extra_info'] = grouped.apply(
    lambda r: [r['gpu_cc'], r['gpu_mem'], *r['notes']], axis=1
)

grouped['extra_info'] = grouped['extra_info'].apply(lambda x: [v for v in x if v != "None"])
grouped['flag'] = grouped['flag'].map({'S':'Shared', 'B':'Buy In'})

output_cols = group_cols + ['quantity', 'hostnames']
export_data = grouped[output_cols].values.tolist()

# Save in JS display order: [hostnames, processor_type, cores, memory, gpu_type, gpus, flag]
export_data = grouped.apply(
    lambda row: [row['hostnames'], row['processor_type'], row['cores'], row['memory'], row['gpu_type'], row['gpus'], row['flag'], row['extra_info']],
    axis=1
).tolist()

# output to a "js" file, containing just the const array that will be used for the table
with open(output_filename, 'w') as outfile:
    outfile.write("const data = ")
    json.dump(export_data, outfile, indent=2)
    outfile.write(";")
