"""Re-encode and compress netcdf file(s).

.. author:: Valentin Louf <valentin.louf@monash.edu>
"""
import os
import glob
import argparse

import xarray as xr
import dask.bag as db


def nczip(infile):
    """
    Re-encode variables in netCDF file and activate zlib compression.

    Args:
        infile: str
            Input netCDF file to recompress.
    """
    data = xr.open_dataset(infile, decode_times=False)
    outfilename = os.path.basename(infile)
    outfilename = os.path.join(OUTPUT_DIR, outfilename)

    args = dict()
    for k, _ in (data.items()):
        args[k] = {'zlib': True}

    data.to_netcdf(outfilename, encoding=args)
    return None


def main():
    if OUTPUT_DIR == INPUT_DIR:
        raise ValueError("Input and output can not be the same.")

    if os.path.isfile(INPUT_DIR):
        nczip(INPUT_DIR)
        return None

    inpath = os.path.join(INPUT_DIR, "*.nc")
    flist_nc = glob.glob(inpath)
    inpath = os.path.join(INPUT_DIR, "*.cdf")
    flist_cdf = glob.glob(inpath)

    flist = flist_nc + flist_cdf
    if len(flist) == 0:
        print("No file found.")
        return None

    print(f"Found {len(flist)} file(s) to deflate.")

    bag = db.from_sequence(flist).map(nczip)
    bag.compute()

    return None



if __name__ == '__main__':
    parser_description = "A script to re-encode and compress netcdf file(s)."

    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument('-i',
        '--indir',
        dest='indir',
        type=str,
        help='Input netcdf file or directory to (re)compress.',
        required=True)
    parser.add_argument('-o',
        '--output',
        dest='outdir',
        type=str,
        help='Output directory.',
        required=True)

    args = parser.parse_args()
    OUTPUT_DIR = args.outdir
    INPUT_DIR = args.indir
    main()