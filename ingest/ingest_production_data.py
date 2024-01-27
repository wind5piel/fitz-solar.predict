from utils import parse_production_csv

import polars as pl

import os
import shutil
import yaml
 
with open('config.yml', 'r') as f:
    config =yaml.safe_load(f)
    data_settings = config['data_settings']


prod_raw_dir = os.path.join(data_settings['data_dir_path'], data_settings['prod_raw_dir_name'])
processed_dir = os.path.join(prod_raw_dir, data_settings['processed_dir'])
outfile = os.path.join(data_settings['data_dir_path'], data_settings['prod_data_file_name'])

files = os.listdir(prod_raw_dir)

# check if processed directory exists, if not, create it
if not os.path.isdir(processed_dir):
    os.mkdir(processed_dir)

# check if outfile exists, if not, create it with first file
if not os.path.exists(outfile):
    file = files.pop(0)
    parse_production_csv(prod_raw_dir, files.pop(0)).write_parquet(outfile)
    shutil.move(os.path.join(prod_raw_dir, file), os.path.join(processed_dir, file))

#load existing data from outfile
production_data = pl.read_parquet(outfile)

#ingest raw files and append to data
for file in files:
    if file.endswith('.csv'):

        production_data = production_data.vstack(parse_production_csv(prod_raw_dir, file)).unique().sort('datetime')
        shutil.move(os.path.join(prod_raw_dir, file), os.path.join(processed_dir, file))

        

# update outfile
production_data.write_parquet(outfile)

#TODO: remove this control file and creation once process runs smoothly
pl.read_parquet(outfile).write_excel('data/production.xlsx')







