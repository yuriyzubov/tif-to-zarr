from tifffile import memmap, imread, imwrite
import numpy as np
import zarr
import os
import multiprocessing as mp


def write_tile_to_zarr(
                        chunk_num : int,
                        path_to_tiff_stack : str,
                        zarray : zarr.Array,
                        tiles_list : list):
    
    slab_shape =  [zarray.chunks[0]] + list(zarray.shape[-2:])
    numpy_slab = np.empty(slab_shape, zarray.dtype, )
    # combine tiles into a slab with thickness equal to the chunk size in z direction
    for slab_index in np.arange(chunk_num, chunk_num+zarray.chunks[0], 1):
        try:
            print(slab_index)
            path_to_tile = os.path.join(path_to_tiff_stack, tiles_list[slab_index])
        except:
            print(f'Tiff tile with index {slab_index} is not present in tiff stack.')
            return
        image_tile = imread(path_to_tile)
        numpy_slab[slab_index - chunk_num, :, :] = image_tile
        
    # write slab into zarr
    zarray[chunk_num : chunk_num+ zarray.chunks[0], :, :] = numpy_slab 
    
    
# multiprocess writing tiff stack into zarr array
def write_tiles_strobbing(path_to_stack : str,
                          zarray : zarr.Group,
                           tiles_list : list,
                           ):
    chunks_list = np.arange(0, zarray.shape[0], zarray.chunks[0])
    print(chunks_list)
    cpu_num=mp.cpu_count()
    with mp.Pool(processes=int(cpu_num * 0.6)) as pool:
        results = pool.starmap(write_tile_to_zarr, ((chunk_num, path_to_stack, zarray, tiles_list) for chunk_num in chunks_list))
    
    return 0