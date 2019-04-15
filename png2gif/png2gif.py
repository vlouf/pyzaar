"""A script to zip directories using multiprocessing.

.. author:: Valentin Louf <valentin.louf@monash.edu>
"""
import os
import glob
import argparse

import imageio


def png2gif(flist, outname):
    """
    Function that transform a list of image file (PNG, JPEG, ...) into an
    animated GIF.

    Parameters:
    ===========
        flist: list[str,]
            List of input files.
        outname: str
            Output file name.
    """
    with imageio.get_writer(outname, mode='I', duration=0.3) as writer:  # Image streamer
        for infile in flist:
            image = imageio.imread(infile)
            writer.append_data(image)

    print(f"{outname} written.")
    return None


def main():
    if not os.path.exists(INPUT_DIR):
        raise FileNotFoundError(f"Input directory {INPUT_DIR} does not exists.")
    if os.path.exists(OUTPUT_FILE):
        raise FileExistsError(f"Output file {OUTPUT_FILE} already exists.")

    inpath = os.path.join(INPUT_DIR, "*.png")
    flist = sorted(glob.glob(inpath))

    # Checking if list empty
    if len(flist) == 0:
        raise FileNotFoundError(f"No PNG file founf in {INPUT_DIR}.")

    png2gif(flist, OUTPUT_FILE)

    return None


if __name__ == '__main__':
    parser_description = "A script to transform a directory of pngs into a gif."

    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument('-i',
        '--indir',
        dest='indir',
        type=str,
        help='Input directory.',
        required=True)
    parser.add_argument('-o',
        '--output',
        dest='outfilename',
        type=str,
        help='Output file.',
        required=True)

    args = parser.parse_args()
    OUTPUT_FILE = args.outfilename
    INPUT_DIR = args.indir
    main()