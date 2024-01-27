from utils import parse_production_csv

import polars as pl

import os
import shutil
import yaml


with open('config.yml', 'r') as f:
    config =yaml.safe_load(f)


prod_raw_dir = os.path.join(config['data_settings']['data_dir_path'], config['data_settings']['prod_raw_dir_name'])
processed_dir = os.path.join(prod_raw_dir, config['data_settings']['processed_dir'])
outfile = os.path.join(config['data_settings']['data_dir_path'], config['data_settings']['prod_data_file_name'])

#TODO: make the following a function to be impoorted and executed on the top level

def ingest_production_data(prod_raw_dir):
    files = os.listdir(prod_raw_dir)

    if not os.path.exists(outfile):
        parse_production_csv(prod_raw_dir, files.pop(0)).write_parquet(outfile)

    if not os.path.isdir(processed_dir):
        os.mkdir(processed_dir)

    production_data = pl.read_parquet(outfile)

    for file in files:
        if file.endswith('.csv'):

            production_data = production_data.vstack(parse_production_csv(prod_raw_dir, file)).unique().sort('datetime')

            shutil.move(os.path.join(prod_raw_dir, file), os.path.join(processed_dir, file))

    production_data.write_parquet(outfile)
    
    #TODO: remove this control file and creation once process runs smoothly
    pl.read_parquet(outfile).write_excel('production.xlsx')







