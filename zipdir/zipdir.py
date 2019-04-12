"""A script to zip directories in multiprocessing.

.. author:: Valentin Louf <valentin.louf@monash.edu>
"""
import os
import glob
import zipfile

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


def process_directory(date, inpath='/g/data2/rr5/arm/data/big_data/access_c2/'):
    """
    As the data that I want to zip are stored in daily directorys, I want to
    create a day by day zip (e.g.: /root/20190412/). This function zip a
    directory for a given date.

    Args:
    =====
        date: str
            A given date of the form YYYYMMDD

    Kwargs:
    =======
        Inpath: str
            Path to root directory.
    """
    indir = os.path.join(inpath, date)
    outfile = os.path.join(inpath, date + '.zip')
    if not os.path.exists(indir):
        print('Input directory {indir} does not exist.')
        return None
    if os.path.isfile(outfile):
        print('Zip file already exists.')
        return None

    print(f'Input directory is {indir} and output zip file is {outfile}.')

    with zipfile.ZipFile(outfile, 'w', zipfile.ZIP_DEFLATED) as zid:
        zipdir(indir, zid)
    return None


def main():
    dtime = pd.date_range('20141114', '20150430')
    datestr = [d.strftime('%Y%m%d') for d in dtime]

    bag = db.from_sequence(datestr).map(process_directory)
    bag.compute()

    return None


if __name__ == '__main__':
    main()