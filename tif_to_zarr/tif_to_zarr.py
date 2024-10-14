"""This module contains all necessary code to run tiff to zarr conversion script in a command line interface mode, using Click python library."""

import conversion_lib as conv
import click
import numpy as np


@click.command()
@click.option(
    "--src", "-s", type=click.Path(exists=True), help="Input .tiff file location."
)
@click.option("--dest", "-d", type=click.Path(), help="Output .tiff file location.")
@click.option(
    "--axes",
    "-a",
    nargs=3,
    default=("z", "y", "x"),
    type=str,
    help="Metadata axis names. Order matters. \n Example: -a z y x",
)
@click.option(
    "--translation",
    "-t",
    nargs=3,
    default=(0.0, 0.0, 0.0),
    type=float,
    help="Metadata translation(offset) value. Order matters. \n Example: -t 1.0 2.0 3.0",
)
@click.option(
    "--scale",
    "-s",
    nargs=3,
    default=(1.0, 1.0, 1.0),
    type=float,
    help="Metadata scale value. Order matters. \n Example: -s 1.0 2.0 3.0",
)
@click.option(
    "--units",
    "-u",
    nargs=3,
    default=("nm", "nm", "nm"),
    type=str,
    help="Metadata unit names. Order matters. \n Example: -t nanometer nanometer nanometer",
)
def tif_to_zarr(src, dest, axes, translation, scale, units):
    """Accept input parameters from click python cli and convert input tiff to ome-ngff zarr.

    Args:
        src (str): path to source tiff file
        dest (str): path to the output zarr file.
        axes (List[str]): list of axes to store in metadata. Order matters.
        translation (List[float]): list of coordinates where the top left corner of the output zarr array should be located when displayed in neuroglancer. Order matters
        scale (List[float]): physical size of the voxel (in units). Order matters.
        units (List[str]): physical dimension units that define in which units the scale attribute is measured. Order matters.
    """
    # load tiff data
    c = conv.Conversion(src, dest, axes, translation, scale, units)
    tiff_data = c.read_tiff()
    # transpose tiff array
    # tiff_data_mod = [np.transpose(tiff_data[0]), tiff_data[1]]
    # flip values along y axis
    tiff_data_mod = [tiff_data[0], tiff_data[1]]
    # tiff_data_mod = [np.flip(tiff_data[0], axis=1), tiff_data[1]]
    print(tiff_data_mod[0].shape)
    # store tiff data in a .zarr file
    chunks = (64, 64, 64)
    c.dask_to_zarray(tiff_data_mod, chunks)


if __name__ == "__main__":
    tif_to_zarr()
