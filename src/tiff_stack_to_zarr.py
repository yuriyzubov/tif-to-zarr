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


@click.command()
@click.option('--src','-s',type=click.Path(exists = True),help='Input tiff stack directory path.')
@click.option('--dest','-s',type=click.STRING,help='Output .zarr file path.')
@click.option('--workers','-w',default=100,type=click.INT,help = "Number of dask workers")
@click.option('--cluster', '-c', default='' ,type=click.STRING, help="Which instance of dask client to use. Local client - 'local', cluster 'lsf'")
def cli(src, dest, workers, cluster):
    
    # if cluster == '':
    #     print('Did not specify which instance of the dask client to use!')
    #     sys.exit(0)
    # elif cluster == 'lsf':
    #     num_cores = 1
    #     cluster = LSFCluster(
    #         cores=num_cores,
    #         processes=num_cores,
    #         memory=f"{15 * num_cores}GB",
    #         ncpus=num_cores,
    #         mem=15 * num_cores,
    #         walltime="48:00",
    #         local_directory = "/scratch/$USER/"
    #         )
    
    # elif cluster == 'local':
    #         cluster = LocalCluster()
    
    # client = Client(cluster)
    # with open(os.path.join(os.getcwd(), "dask_dashboard_link" + ".txt"), "w") as text_file:
    #     text_file.write(str(client.dashboard_link))
    # print(client.dashboard_link)

    path_to_stack = src
    z_store = zarr.DirectoryStore(dest)
    tiff_stack = natsorted(os.listdir(path_to_stack))

    probe_image = imread(os.path.join(path_to_stack, tiff_stack[0]))
    
    z_root = zarr.open(store=z_store, mode = 'a')
    tiff_arr_shape = [len(tiff_stack)] + list(probe_image.shape)
    z_arr = z_root.require_dataset(name = 's0', 
                        shape = tiff_arr_shape,  
                        dtype = probe_image.dtype,
                        chunks = (13, 128, 128),
                        compressor = Zstd(level=6))
        
        
    start_time = time.time()
    print(z_arr.chunks[0])
    print(len(tiff_stack))
    # if tiff stack fits into one chunk, choose max value of zarr stack as the length of the tiff stack
    zarr_stack = min(z_arr.chunks[0], len(tiff_stack))
    print(zarr_stack)
    
    write_tiles_strobbing(path_to_stack, z_arr, tiff_stack)
        
    print(time.time() - start_time)
if __name__ == '__main__':
    cli()
