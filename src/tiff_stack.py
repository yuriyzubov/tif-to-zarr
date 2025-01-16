from tifffile import imread
import numpy as np
import zarr
import os
from dask.distributed import Client, wait
import time
import dask.array as da
from natsort import natsorted
from glob import glob
from tiff_volume import TiffVolume


class TiffStack(TiffVolume):

    def __init__(
        self,
        src_path: str,
        axes: list[str],
        scale: list[float],
        translation: list[float],
        units: list[str],
    ):
        """Construct all the necessary attributes for the proper conversion of tiff to OME-NGFF Zarr.

        Args:
            input_filepath (str): path to source tiff file.
        """
        self.src_path = src_path
        self.stack_list = natsorted(glob(os.path.join(src_path, "*.tif*")))
        probe_image_store = imread(
            os.path.join(src_path, self.stack_list[0]), aszarr=True
        )
        probe_image_arr = da.from_zarr(probe_image_store)

        self.dtype = probe_image_arr.dtype
        self.shape = [len(self.stack_list)] + list(probe_image_arr.shape)

        # metadata
        self.zarr_metadata = {
            "axes": axes,
            "translation": translation,
            "scale": scale,
            "units": units,
        }

    def write_tile_slab_to_zarr(
        self, chunk_num: int, zarray: zarr.Array, src_volume: list
    ):

        # check if the slab is at the array boundary or not
        if chunk_num + zarray.chunks[0] > zarray.shape[0]:
            slab_thickness = zarray.shape[0] - chunk_num
        else:
            slab_thickness = zarray.chunks[0]

        slab_shape = [slab_thickness] + list(zarray.shape[-2:])
        np_slab = np.empty(slab_shape, zarray.dtype)

        # combine tiles into a slab with thickness equal to the chunk size in z direction
        for slab_index in np.arange(chunk_num, chunk_num + slab_thickness, 1):
            try:
                image_tile = imread(src_volume[slab_index])
            except:
                print(
                    f"Tiff tile with index {slab_index} is not present in tiff stack."
                )
            np_slab[slab_index - chunk_num, :, :] = image_tile

        # write a tiff stack slab into a zarr array
        zarray[chunk_num : chunk_num + zarray.chunks[0], :, :] = np_slab

    # parallel writing of tiff stack into zarr array
    def write_to_zarr(self, zarray: zarr.Array, client: Client):
        chunks_list = np.arange(0, zarray.shape[0], zarray.chunks[0])
        print(chunks_list)

        start = time.time()
        fut = client.map(
            lambda v: self.write_tile_slab_to_zarr(v, zarray, self.stack_list),
            chunks_list,
        )
        print(
            f"Submitted {len(chunks_list)} tasks to the scheduler in {time.time()- start}s"
        )

        # wait for all the futures to complete
        result = wait(fut)
        print(f"Completed {len(chunks_list)} tasks in {time.time() - start}s")

        return 0
