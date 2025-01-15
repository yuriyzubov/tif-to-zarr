from tifffile import imread, imwrite
import zarr
import multiprocessing as mp
import numpy as np
from numcodecs import Zstd
from writes import write_tiles_strobbing
import os
from natsort import natsorted
import click
import sys
from dask_jobqueue import LSFCluster
from dask.distributed import Client, LocalCluster
import multiprocessing as mp
import time
from glob import glob
import dask.array as da


@click.command()
@click.option('--src','-s',type=click.Path(exists = True),help='Input tiff file location, or tiff stack directory path.')
@click.option('--dest','-s',type=click.STRING,help='Output .zarr file path.')
@click.option('--num_workers','-w',default=100,type=click.INT,help = "Number of dask workers")
@click.option('--cluster', '-c', default='' ,type=click.STRING, help="Which instance of dask client to use. Local client - 'local', cluster 'lsf'")
@click.option('--zarr_chunks', '-zc', nargs=3, default=(64, 128, 128), type=click.INT, help='Chunk size for (z, y, x) axis order. z-axis is normal to the tiff stack plane. Default (64, 128, 128)')
def cli(src, dest, num_workers, cluster, zarr_chunks):
    
    # create a dask client to submit tasks
    if cluster == '':
        print('Did not specify which instance of the dask client to use!')
        sys.exit(0)
    elif cluster == 'lsf':
        num_cores = 1
        cluster = LSFCluster(
            cores=num_cores,
            processes=num_cores,
            memory=f"{15 * num_cores}GB",
            ncpus=num_cores,
            mem=15 * num_cores,
            walltime="48:00",
            local_directory = "/scratch/$USER/"
            )
    
    elif cluster == 'local':
            cluster = LocalCluster()
    
    client = Client(cluster)
    with open(os.path.join(os.getcwd(), "dask_dashboard_link" + ".txt"), "w") as text_file:
        text_file.write(str(client.dashboard_link))
    print(client.dashboard_link)
    
    if os.path.isdir(src):
        tiff_type = 'stack'
    elif src.endswith('.tif') or src.endswith('.tiff'):
        tiff_type = 'volume' 
    
    if tiff_type == 'stack':
        
        src_volume = natsorted(glob(os.path.join(src, '*.tif*')))
        probe_image_store = imread(os.path.join(src, src_volume[0]), aszarr=True)
        probe_image_arr = da.from_zarr(probe_image_store)
        
        tiff_3d_shape = [len(src_volume)] + list(probe_image_arr.shape)
        tiff_3d_dtype = probe_image_arr.dtype
        
    elif tiff_type == 'volume':
        src_volume = src
        tiff_3d_store = imread(os.path.join(src), aszarr=True)
        tiff_volume = da.from_zarr(tiff_3d_store) 
        print(type(tiff_volume))
        
        tiff_3d_shape = tiff_volume.shape
        tiff_3d_dtype = tiff_volume.dtype
        
    z_store = zarr.NestedDirectoryStore(dest)
    z_root = zarr.open(store=z_store, mode = 'a')
    z_arr = z_root.require_dataset(name = 's0', 
                        shape = tiff_3d_shape,  
                        dtype = tiff_3d_dtype,
                        chunks = zarr_chunks,
                        compressor = Zstd(level=6))
        
        
    start_time = time.time()
    
    client.cluster.scale(num_workers)
    write_tiles_strobbing(z_arr, src_volume, client)
    client.cluster.scale(0)
        
    print(time.time() - start_time)
if __name__ == '__main__':
    cli()
