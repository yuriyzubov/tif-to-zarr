from tifffile import imread
import numpy as np
import zarr
import os
from dask.distributed import Client, wait
import time
import dask.array as da


def write_tileslab_to_zarr(
                        chunk_num : int,
                        zarray : zarr.Array,
                        src_volume : list | str):
    
    # check if the slab is at the array boundary or not
    if chunk_num + zarray.chunks[0] > zarray.shape[0]:
        slab_thickness = zarray.shape[0] - chunk_num 
    else:
        slab_thickness = zarray.chunks[0]
    
    slab_shape =  [slab_thickness] + list(zarray.shape[-2:])
    np_slab = np.empty(slab_shape, zarray.dtype)
    
    if isinstance(src_volume, list): # for a tiff stack
        # combine tiles into a slab with thickness equal to the chunk size in z direction
        for slab_index in np.arange(chunk_num, chunk_num+slab_thickness, 1):
            try:
                print(slab_index)
                image_tile = imread(src_volume[slab_index])
            except:
                print(f'Tiff tile with index {slab_index} is not present in tiff stack.')
            np_slab[slab_index - chunk_num, :, :] = image_tile

    elif isinstance(src_volume, str): # for a 3D tiff file
        print(np_slab.shape)
        tiff_slab = imread(src_volume, key=range(chunk_num, chunk_num + slab_thickness, 1))
        print(tiff_slab.shape)
        np_slab[0 : zarray.chunks[0], :, :] = tiff_slab
    
    # write a tiff stack slab into zarr array        
    zarray[chunk_num : chunk_num+ zarray.chunks[0], :, :] = np_slab

    
    
# multiprocess writing tiff stack into zarr array
def write_tiles_strobbing(zarray : zarr.Group,
                           src_volume : list | str,
                           client : Client
                           ):
    chunks_list = np.arange(0, zarray.shape[0], zarray.chunks[0])
    print(chunks_list)

    start = time.time()
    fut = client.map(lambda v: write_tileslab_to_zarr(v, zarray, src_volume), chunks_list)
    print(f'Submitted {len(chunks_list)} tasks to the scheduler in {time.time()- start}s')
    
    # wait for all the futures to complete
    result = wait(fut)
    print(f'Completed {len(chunks_list)} tasks in {time.time() - start}s')
    
    return 0

