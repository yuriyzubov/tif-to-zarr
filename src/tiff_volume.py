from tifffile import imread
import numpy as np
import zarr
import os
from dask.distributed import Client, wait
import time
import dask.array as da
import copy

class TiffVolume():

    def __init__(self,
                src_path: str):
        """Construct all the necessary attributes for the proper conversion of tiff to OME-NGFF Zarr.

        Args:
            input_filepath (str): path to source tiff file.
        """
        self.src_path = src_path
        
        self.zarr_store = imread(os.path.join(src_path), aszarr=True)
        self.zarr_arr = zarr.open(self.zarr_store) 
        print(type(self.zarr_arr))
        
        self.shape = self.zarr_arr.shape
        self.dtype = self.zarr_arr.dtype
            
    # multiprocess writing tiff stack into zarr array
    def write_to_zarr(self,
                    zarray : zarr.Group,
                    client : Client
                    ):
        chunks_list = np.arange(0, zarray.shape[0], zarray.chunks[0])
        print(chunks_list)
        
        src_path = copy.copy(self.src_path)

        start = time.time()
        fut = client.map(lambda v: write_volume_slab_to_zarr(v, zarray, src_path), chunks_list)
        print(f'Submitted {len(chunks_list)} tasks to the scheduler in {time.time()- start}s')
        
        # wait for all the futures to complete
        result = wait(fut)
        print(f'Completed {len(chunks_list)} tasks in {time.time() - start}s')
        
        return 0

def write_volume_slab_to_zarr(
                            chunk_num : int,
                            zarray : zarr.Array,
                            src_path : str
                            ):
    
    # check if the slab is at the array boundary or not
    if chunk_num + zarray.chunks[0] > zarray.shape[0]:
        slab_thickness = zarray.shape[0] - chunk_num 
    else:
        slab_thickness = zarray.chunks[0]
    
    slab_shape =  [slab_thickness] + list(zarray.shape[-2:])
    np_slab = np.empty(slab_shape, zarray.dtype)
    
    tiff_slab = imread(src_path, key=range(chunk_num, chunk_num + slab_thickness, 1))
    np_slab[0 : zarray.chunks[0], :, :] = tiff_slab
    
    # write a tiff stack slab into zarr array        
    zarray[chunk_num : chunk_num+ zarray.chunks[0], :, :] = np_slab