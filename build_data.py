import pandas as pd
import json

# load nodes data file
df = pd.read_csv("/projectnb/rcsmetrics/nodes/data/nodes.csv")
extra_notes = pd.read_csv("extrainfo.csv")

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
cpu_display_map = {
    'Gold-6242': '<a href="https://ark.intel.com/content/www/us/en/ark/products/192440/intel-xeon-gold-6242-processor-22m-cache-2-80-ghz.html" target="_blank">Intel Gold 6242</a>',
    'E5-2407v2': 'Intel E5-2407v2',
    '7250': '<a href="https://www.intel.com/content/www/us/en/products/sku/94035/intel-xeon-phi-processor-7250-16gb-1-40-ghz-68-core/specifications.html?wapkw=xeon%20phi%207250" target="_blank">Intel Xeon Phi (Knights Landing) 7250</a>',
    'E5-2680v4': '<a href="https://www.intel.com/content/www/us/en/products/sku/91754/intel-xeon-processor-e52680-v4-35m-cache-2-40-ghz/specifications.html?wapkw=e5-2680%20v4" target="_blank">Intel Xeon E5-2680v4</a>',
    'EPYC-7501': 'AMD EPYC-7501',
    'EPYC-7702': 'AMD EPYC-7501',
    'E5-2670': '<a href="https://www.intel.com/content/www/us/en/products/sku/64595/intel-xeon-processor-e52670-20m-cache-2-60-ghz-8-00-gts-intel-qpi/specifications.html?wapkw=e5-2670" target="_blank">Intel Xeon E5-2670</a>',
    'E5-2650v2': '<a href="https://www.intel.com/content/www/us/en/products/sku/75269/intel-xeon-processor-e52650-v2-20m-cache-2-60-ghz/specifications.html?wapkw=xeon%20e5%202650%20v2" target="_blank">Intel Xeon E5-2650v2</a>',
    'E5-2660v3': '<a href="https://www.intel.com/content/www/us/en/products/sku/81706/intel-xeon-processor-e52660-v3-25m-cache-2-60-ghz/specifications.html?wapkw=e5-2660%20v3" target="_blank">Intel Xeon E5-2660v3</a>',
    'E7-4809v3': '<a href="https://www.intel.com/content/www/us/en/products/sku/84676/intel-xeon-processor-e74809-v3-20m-cache-2-00-ghz/specifications.html?wapkw=e7-4809%20" target="_blank">Intel Xeon E7-4809v3</a>',
    'E7-8867v4': '<a href="https://www.intel.com/content/www/us/en/products/sku/93804/intel-xeon-processor-e78867-v4-45m-cache-2-40-ghz/specifications.html?wapkw=e7-8867" target="_blank">Intel Xeon E7-8867v4</a>',
    'E5-2680': '<a href="https://www.intel.com/content/www/us/en/products/sku/64583/intel-xeon-processor-e52680-20m-cache-2-70-ghz-8-00-gts-intel-qpi/specifications.html?wapkw=e5-2680" target="_blank">Intel Xeon E5-2680</a>',
    'Gold-6226R': '<a href="https://ark.intel.com/content/www/us/en/ark/products/199347/intel-xeon-gold-6226r-processor-22m-cache-2-90-ghz.html" target="_blank">Intel Xeon Gold 6226R</a>',
    'Gold-5120': '<a href="https://ark.intel.com/content/www/us/en/ark/products/120474/intel-xeon-gold-5120-processor-19-25m-cache-2-20-ghz.html" target="_blank">Intel Xeon Gold 5120</a>',
    'Gold-5118': '<a href="https://ark.intel.com/content/www/us/en/ark/products/120473/intel-xeon-gold-5118-processor-16-5m-cache-2-30-ghz.html" target="_blank">Intel Xeon Gold 5118</a>',
    'Gold-6326': '<a href="https://www.intel.com/content/www/us/en/products/sku/215274/intel-xeon-gold-6326-processor-24m-cache-2-90-ghz/specifications.html" target="_blank">Intel Gold 6326</a>',
    'Gold-6132': '<a href="https://www.intel.com/content/www/us/en/products/sku/123541/intel-xeon-gold-6132-processor-19-25m-cache-2-60-ghz/specifications.html?wapkw=intel%20gold%206132" target="_blank">Intel Gold 6132</a>',
    'Gold-6426Y': '<a href="https://www.intel.com/content/www/us/en/products/sku/232377/intel-xeon-gold-6426y-processor-37-5m-cache-2-50-ghz/specifications.html" target="_blank">Intel Gold 6426Y</a>',
    'EPYC-7351': '<a href="https://www.amd.com/en/support/downloads/drivers.html/processors/epyc/epyc-7001-series/amd-epyc-7351.html" target="_blank">AMD Epyc 7351</a>',
    'E5-2620v3': '<a href="https://www.intel.com/content/www/us/en/products/sku/83352/intel-xeon-processor-e52620-v3-15m-cache-2-40-ghz/specifications.html?wapkw=e5-2620%20v3" target="_blank">Intel Xeon E5-2620v3</a>',
    'E5-2620v4': '<a href="https://www.intel.com/content/www/us/en/products/sku/92986/intel-xeon-processor-e52620-v4-20m-cache-2-10-ghz/specifications.html?wapkw=e5-2620%20v4" target="_blank">Intel Xeon E5-2620v4</a>',
    'Platinum-8468': '<a href="https://www.intel.com/content/www/us/en/products/sku/231735/intel-xeon-platinum-8468-processor-105m-cache-2-10-ghz/specifications.html" target="_blank">Intel Platinum 8468</a>',
    'EPYC-7413': '<a href="https://www.amd.com/en/products/processors/server/epyc/7003-series/amd-epyc-7413.html" target="_blank">AMD EPYC-7413</a>',
    'EPYC-7302': '<a href="https://www.amd.com/en/support/downloads/drivers.html/processors/epyc/epyc-7002-series/amd-epyc-7302.html" target="_blank">AMD EPYC-7302</a>',
    'EPYC-9354': '<a href="https://www.amd.com/en/products/processors/server/epyc/4th-generation-9004-and-8004-series/amd-epyc-9354.html" target="_blank">AMD EPYC 9354</a>',
    'Platinum-8358': '<a href="https://www.intel.com/content/www/us/en/products/sku/212282/intel-xeon-platinum-8358-processor-48m-cache-2-60-ghz/specifications.html" target="_blank">Intel Xeon Platinum 8358</a>',
    'Gold-6526Y': '<a href="https://www.intel.com/content/www/us/en/products/sku/237560/intel-xeon-gold-6526y-processor-37-5m-cache-2-80-ghz/specifications.html" target="_blank">Intel Gold 6526Y</a>',
    'EPYC-9124': '<a href="https://www.amd.com/en/products/processors/server/epyc/4th-generation-9004-and-8004-series/amd-epyc-9124.html" target="_blank">EPYC 9124</a>',
    'EPYC-9135': '<a href="https://www.amd.com/en/products/processors/server/epyc/9005-series/amd-epyc-9135.html" target="_blank">EPYC 9135</a>',
}

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
with open("data.js", 'w') as outfile:
    outfile.write("const data = ")
    json.dump(export_data, outfile, indent=2)
    outfile.write(";")
