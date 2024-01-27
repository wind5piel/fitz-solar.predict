from .date_and_time import generate_full_datetime

import polars as pl

from datetime import datetime, timedelta
import os
import re

# TODO: outsource parameters to params file
data_columns = {
    'datetime': pl.Datetime,
    'production': pl.Float64,
    'prod_unit': pl.Utf8,
    'savings': pl.Float64,
    'save_unit': pl.Utf8
    }

def parse_production_csv(folder, file) -> pl.DataFrame:
    """
    Parse a CSV file and return a DataFrame.

    Args:
        folder (str): The folder path where the CSV file is located.
        file (str): The name of the CSV file to parse.

    Returns:
        pl.DataFrame: The parsed CSV data as a DataFrame, with updated/complete datetime column.

    """

    # read csv
    daily_data = pl.read_csv(source=os.path.join(folder, file), skip_rows=1, separator=';')

    # generate general datetime, as file comes with time only and date in filename
    filename_nums = re.findall(r'\d+',file)
    file_date_str = filename_nums[0]
    file_time_str = filename_nums[1]

    date_obj = datetime.strptime(file_date_str, '%Y%m%d')
    file_time_obj = datetime.strptime(file_time_str, '%H%M%S')

    file_creation_datetime = datetime.combine(date_obj.date(), file_time_obj.time())


    datetimes_raw = [generate_full_datetime(date_obj, time_str) for time_str in daily_data['Date/Time']]
    datetimes = [dt if dt < file_creation_datetime else dt - timedelta(days=1) for dt in datetimes_raw]

    datetimes_df = pl.DataFrame({'datetime':datetimes})

    # output relevant columns only
    relevant_data = daily_data.columns[1:5]
    output_df = datetimes_df.hstack(daily_data.select(relevant_data))
    output_df.columns = [c for c in data_columns.keys()]

    # convert string to numeric
    for col in data_columns:
        if data_columns[col] in [pl.Int64, pl.Float64]:
            output_df = output_df.with_columns(pl.col(col).str.replace(',', '.').cast(pl.Float64))

    return output_df