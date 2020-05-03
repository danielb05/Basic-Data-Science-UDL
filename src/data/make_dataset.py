# -*- coding: utf-8 -*-
import click
import json
import shutil
import logging
from pathlib import Path
from functools import partial
from dotenv import find_dotenv, load_dotenv

import pandas as pd


@click.group()
def main():
    pass


@main.command()
@click.argument('input_filepath', type=click.Path(exists=True, dir_okay=False),
                default='data/raw/iris.data')
@click.argument('output_filepath', type=click.Path(dir_okay=False),
                default='data/processed/iris.csv')
def iris(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    columns = ['sepal_length', 
               'sepal_width', 
               'petal_length', 
               'petal_width', 
               'class']

    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw iris data')

    logger.info(f'reading source file from {input_filepath}')
    df = pd.read_csv(input_filepath, header=None, names=columns)
    
    logger.info(f'writting new csv file to {output_filepath}')
    df.to_csv(output_filepath, index=False)


@main.command()
@click.argument('input_directory', 
                type=click.Path(exists=True, file_okay=False),
                default='data/raw/measures')
@click.argument('output_directory', type=click.Path(file_okay=False),
                default='data/processed/measures')
@click.argument('interim_directory', type=click.Path(file_okay=False),
                default='data/interim/measures')
def measures(input_directory, output_directory, interim_directory):
    def read_json_lines(f: str) -> pd.DataFrame:
        lines = Path(f).read_text().split('\n')
        json_data = [json.loads(o, encoding='utf-8') for o in lines if o]
        return pd.DataFrame(json_data)

    def json_lines_to_csv(fname):
        fname_out = input_dir / f'{fname.stem}.csv'
        df = read_json_lines(fname)
        df.to_csv(fname_out, index=False)

    def join_files(sensor):
        reads = [(read_sensor(o), o.stem.split('-')[0]) 
                 for o in interim_dir.glob(f'*-{s}-*')]
        dfs, units = zip(*reads)

        df = pd.merge(*dfs, 'left', 'time')
        df.rename(columns=dict(
            value_x=units_mapper[units[0]],
            value_y=units_mapper[units[1]]), inplace=True)

        df['sensor'] = s
        df.fillna(0., inplace=True)

        return df

    logger = logging.getLogger(__name__)

    logger.info('Creating directories structures')
    input_dir = Path(input_directory)
    output_dir = Path(output_directory)
    interim_dir = Path(interim_directory)
    
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    interim_dir.mkdir(exist_ok=True)

    logger.info('Creating intermediate CSV files')

    for f in input_dir.glob('*.json'):
        json_lines_to_csv(f)

    for f in input_dir.glob('*.csv'):
        fname_out = interim_dir / f'{f.stem}.csv'
        shutil.copy(str(f), str(fname_out))
    
    logger.info('Creating final CSV sensor files')
    units_mapper = dict(H='humidity', T='temperature', P='pressure')
    read_sensor =  partial(pd.read_csv, 
                           index_col='time', 
                           usecols=['time', 'value'])

    sensors = set(o.stem.split('-')[1] for o in interim_dir.iterdir())

    for s in list(sensors):
        logger.info(f'Joining sensor {s} files...')
        s_df = join_files(s)
        s_df.to_csv(output_dir / f'{s}.csv')


@main.command()
@click.argument('input_directory', 
                type=click.Path(exists=True, file_okay=False),
                default='data/raw/cryptocurrencypricehistory')
@click.argument('output_directory', type=click.Path(file_okay=False),
                default='data/processed/cryptocurrencypricehistory')
def cryptocurrency(input_directory, output_directory):
    logger = logging.getLogger(__name__)

    logger.info('Creating directories structures')
    input_dir = Path(input_directory)
    output_dir = Path(output_directory)
    
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    col_list = ["Date", "Close"]
    num = 1

    files = input_dir.glob('*.csv')

    df_cc = pd.read_csv(input_dir / 'bitcoin_cash_price.csv', 
                        index_col='Date', usecols=col_list)
    df_cc.rename(columns={df_cc.columns[0]:'bitcoin_cash_price'}, inplace=True)

    for filename in files:
        if (filename.stem != 'bitcoin_dataset' and 
                filename.stem != 'ethereum_dataset'):
            
            nf = filename.stem
            df = pd.read_csv(filename, index_col='Date', usecols=col_list)
            df_cc = df_cc.merge(df, 'left', 'Date')

            df_cc.rename(columns={df_cc.columns[num]:nf}, inplace=True)

            num += 1

    df_bit = pd.read_csv(input_dir / 'bitcoin_dataset.csv', index_col='Date')
    df_eth = pd.read_csv(input_dir / 'ethereum_dataset.csv', 
                         index_col='Date(UTC)')


    df_cc.fillna(0., inplace=True)
    df_bit.fillna(0., inplace=True)
    df_eth.fillna(0., inplace=True)
    
    df_cc.to_csv(output_dir / 'cryptocurrency_close_values.csv')
    df_bit.to_csv(output_dir / 'bitcoin_dataset.csv')
    df_eth.to_csv(output_dir / 'ethereum_dataset.csv')


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

