# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
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


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

