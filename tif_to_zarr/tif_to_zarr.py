import tif_to_zarr.conversion_lib as conv
import click
import numpy as np

@click.command("tif_to_zarr")
@click.option('--src', '-s', type=click.Path(exists = True), help='Input .tiff file location.')
@click.option("--dest", '-d', type=click.Path(), help = 'Output .tiff file location.')
@click.option('--axes', '-a',nargs=3, default =("z", "y", "x"), type=str, help = "Metadata axis names. Order matters. \n Example: -a z y x")
@click.option('--translation', '-t', nargs=3, default =(0.0, 0.0, 0.0), type=float, help = "Metadata translation(offset) value. Order matters. \n Example: -t 1.0 2.0 3.0")
@click.option('--scale', '-s', nargs=3, default = (1.0, 1.0, 1.0), type=float,  help = "Metadata scale value. Order matters. \n Example: -s 1.0 2.0 3.0")
@click.option('--units', '-u',nargs=3, default =("nm", "nm", "nm"), type=str,  help = "Metadata unit names. Order matters. \n Example: -t nanometer nanometer nanometer")
def tif_to_zarr(src, dest, axes, translation, scale, units):
    # load tiff data
    c = conv.Conversion(src, dest,  axes, translation, scale, units)
    tiff_data = c.read_tiff()
    #transpose tiff array
    #tiff_data_mod = [np.transpose(tiff_data[0]), tiff_data[1]]
    print(tiff_data[0].shape)
    # store tiff data in a .zarr file  
    chunks = (128, 128, 128)
    c.dask_to_zarray(tiff_data, chunks)    
    
# #if __name__ == '__main__':
# @click.group('tif_to_zarr')
#     cli()
