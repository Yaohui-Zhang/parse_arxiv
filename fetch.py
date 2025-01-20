import arxiv
import pandas as pd
import concurrent.futures
from tqdm import tqdm
from doc2json.tex2json.process_tex import process_tex_file
import subprocess
import os
tqdm.pandas()
def apply_in_parallel_no_return(df, func):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        list(tqdm(executor.map(func, [row for _, row in df.iterrows()]), total=len(df)))


def func(row):
    try:
        paper = next(arxiv.Client().results(arxiv.Search(id_list=[row['id']+row['max_version']])))
        paper.download_source(dirpath="./mydir", filename=f"{row['id']}.tar.gz")
        process_tex_file("./mydir/"+f"{row['id']}.tar.gz", 'temp_dir/', 'output_dir/', 'log', False)
        if os.path.exists("./mydir/"+f"{row['id']}.tar.gz"):
            subprocess.run(['rm', "./mydir/"+f"{row['id']}.tar.gz"])
    except Exception as e:
        print(e)
        if os.path.exists("./mydir/"+f"{row['id']}.tar.gz"):
            subprocess.run(['rm', "./mydir/"+f"{row['id']}.tar.gz"])
    
df=pd.read_parquet("4/cs.parquet")
df.progress_apply(func, axis=1)