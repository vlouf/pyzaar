import gc
import os
import glob
import warnings
import argparse
import traceback

import pyart
import numpy as np
import xarray as xr
import dask.bag as db


def stats_refl(infile):
    try:
        radar = pyart.io.read_cfradial(infile, include_fields=['DBZ'])
        unit = np.zeros((240, 500, 3, 3), dtype=np.int8)

        r = radar.range['data']
        dr = 300
        for i in range(3):  # Elevation
            for j in range(3):  # Refl thrshld
                zthresh = 40 + j * 10
                sl = radar.get_slice(i)
                refl = radar.fields['DBZ']['data'][sl]
                azi = radar.azimuth['data'][sl]
                apos, rpos = np.where(refl > zthresh)
                adx = np.round((azi[apos] - azi.min()) / 1.5).astype(int) % 240
                rdx = ((r[rpos] - r[0]) / dr).astype(int)
                unit[adx, rdx, i, j] += 1
        del radar
    except Exception:
        traceback.print_exc()
        unit = None

    return unit


def process_year(year):
    if year == 2009 or year == 2008:
        return None
    flist = sorted(glob.glob(os.path.join(INDIR, f'{year}/**/*.nc')))
    if len(flist) == 0:
        return None
    if os.path.isfile(f'zthresholds_{year}.nc'):
        return None

    bag = db.from_sequence(flist).map(stats_refl)
    rslt = bag.compute()
    rslt = [r for r in rslt if r is not None]

    unit = np.zeros((240, 500, 3, 3), dtype=np.int64)
    rmaskv = np.arange(150, 150e3, 300).astype(np.int32)
    amaskv = np.arange(0, 360, 1.5)

    for r in rslt:
        unit += r

    count_elev_0 = np.squeeze(unit[:, :, 0, :])
    count_elev_1 = np.squeeze(unit[:, :, 1, :])
    count_elev_2 = np.squeeze(unit[:, :, 2, :])

    dset = xr.Dataset({'zstats_elev0': (('azimuth', 'range', 'threshold'), count_elev_0),
                       'zstats_elev1': (('azimuth', 'range', 'threshold'), count_elev_1),
                       'zstats_elev2': (('azimuth', 'range', 'threshold'), count_elev_2),
                       'azimuth': (('azimuth'), amaskv),
                       'range': (('range'), rmaskv),
                       'threshold': (('threshold'), [40, 50, 60])
                      })

    dset.azimuth.attrs = pyart.config.get_metadata('azimuth')
    dset.range.attrs = pyart.config.get_metadata('range')
    dset.threshold.attrs = {'units': 'dBZ', 'description': 'Reflectivity threshold'}
    dset.zstats_elev0.attrs = {'units': '1', 'description': 'Count volumes with reflectivity above threshold'}
    dset.zstats_elev1.attrs = {'units': '1', 'description': 'Count volumes with reflectivity above threshold'}
    dset.zstats_elev2.attrs = {'units': '1', 'description': 'Count volumes with reflectivity above threshold'}

    dset.attrs['total'] = len(flist)
    dset.to_netcdf(os.path.join(OUTDIR, f'zthresholds_{year}.nc'))

    del rslt

    return None


def main():
    try:
        process_year(YEAR)
        gc.collect()
    except Exception:
        traceback.print_exc()

    return None


if __name__ == "__main__":
    parser_description = "Compute statistics of radar PPI reflectivity."
    parser = argparse.ArgumentParser(description=parser_description)

    parser.add_argument('-y',
                        '--year',
                        type=int,
                        dest='year',
                        help='Year to process.',
                        required=True)
    parser.add_argument('-i',
                        '--input-dir',
                        type=str,
                        dest='indir',
                        help='Input directory.',
                        default='/g/data/hj10/admin/cpol_level_1a/v2019/ppi/',
                        required=False)
    parser.add_argument('-o',
                        '--output-dir',
                        type=str,
                        dest='outdir',
                        help='Output directory.',
                        default=os.path.expanduser('~'),
                        required=False)

    args = parser.parse_args()
    YEAR = args.year
    INDIR = args.indir
    OUTDIR = args.outdir

    warnings.simplefilter('ignore')
    main()
