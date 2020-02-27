"""A script to zip directories using multiprocessing.

.. author:: Valentin Louf <valentin.louf@monash.edu>
"""
import os
import glob
import zipfile
import argparse

import dask.bag as db
import pandas as pd


def zipdir(path, ziph):
    """
    Zip all files and subdirectories from a given directory.

    Args:
    =====
        path: str
            Path to the directory to zip
        ziph: ZipFile
            Zip file handle.
    """
    for root, _, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))
    return None


def process_directory(date):
    """
    As the data that I want to zip are stored in daily directorys, I want to
    create a day by day zip (e.g.: /root/20190412/). This function zip a
    directory for a given date.

    Args:
    =====
        date: str
            A given date of the form YYYYMMDD
    """
    indir = os.path.join(INPUT_DIR, str(date.year), date.strftime('%Y%m%d'))
    outdir = os.path.join(OUTPUT_DIR, str(date.year))
    try:
        os.mkdir(outdir)
    except FileExistsError:
        pass
    outfile = os.path.join(OUTPUT_DIR, str(date.year), date.strftime('%Y%m%d') + '.zip')
    if not os.path.exists(indir):
        print(f'Input directory {indir} does not exist.')
        return None
    if os.path.isfile(outfile):
        print('Zip file already exists.')
        return None

    print(f'Input directory is {indir} and output zip file is {outfile}.')

    with zipfile.ZipFile(outfile, 'w', zipfile.ZIP_DEFLATED) as zid:
        zipdir(indir, zid)
    return None


def main():
    dtime = pd.date_range(START_DATE, END_DATE)
#     datestr = [d.strftime('%Y%m%d') for d in dtime]

    bag = db.from_sequence(dtime).map(process_directory)
    bag.compute()

    return None


if __name__ == '__main__':
    parser_description = "A script to zip directories using multiprocessing."

    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument('-s',
        '--start-date',
        dest='start_date',
        default='20141114',
        type=str,
        help='Starting date.')
    parser.add_argument('-e',
        '--end-date',
        dest='end_date',
        default='20150430',
        type=str,
        help='Ending date.')
    parser.add_argument('-i',
        '--indir',
        dest='indir',
        default="/g/data2/rr5/CPOL_radar/CPOL_level_1b/GRIDDED/GRID_70km_1000m/",
        type=str,
        help='Input directory.')
    parser.add_argument('-o',
        '--output',
        dest='outdir',
        default="/g/data/hj10/cpol_level_1b/v2018/gridded/grid_70km_1000m/",
        type=str,
        help='Output directory.')

    args = parser.parse_args()
    START_DATE = args.start_date
    END_DATE = args.end_date
    OUTPUT_DIR = args.outdir
    INPUT_DIR = args.indir
    main()
