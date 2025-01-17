from tifffile import imread
import numpy as np
import zarr
import os
from dask.distributed import Client, wait
import time
import dask.array as da
import copy


class TiffVolume:

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

        self.zarr_store = imread(os.path.join(src_path), aszarr=True)
        self.zarr_arr = zarr.open(self.zarr_store)

        self.shape = self.zarr_arr.shape
        self.dtype = self.zarr_arr.dtype

        # metadata
        self.zarr_metadata = {
            "axes": axes,
            "translation": translation,
            "scale": scale,
            "units": units,
        }

    # multiprocess writing tiff stack into zarr array
    def write_to_zarr(self, zarray: zarr.Group, client: Client):
        chunks_list = np.arange(0, zarray.shape[0], zarray.chunks[0])

        src_path = copy.copy(self.src_path)

        start = time.time()
        fut = client.map(
            lambda v: write_volume_slab_to_zarr(v, zarray, src_path), chunks_list
        )
        print(
            f"Submitted {len(chunks_list)} tasks to the scheduler in {time.time()- start}s"
        )

        # wait for all the futures to complete
        result = wait(fut)
        print(f"Completed {len(chunks_list)} tasks in {time.time() - start}s")

        return 0

    def populate_zarr_attrs(self, root):
        """Add selected tiff metadata to zarr attributes file (.zattrs).

        Args:
            root (zarr.Group): root group of the output zarr array
            zarr_metadata (): combined zarr metadata from input translation, scale, axes names and units
            data_address (str): path to array
        """
        # json template for a multiscale structure
        multscale_dict = {
            "multiscales": [
                {
                    "axes": [],
                    "coordinateTransformations": [
                        {"scale": [1.0, 1.0, 1.0], "type": "scale"}
                    ],
                    "datasets": [
                        {
                            "coordinateTransformations": [
                                {"scale": [], "type": "scale"},
                                {"translation": [], "type": "translation"},
                            ],
                            "path": "unknown",
                        }
                    ],
                    "name": ("/" if root.path == "" else root.path),
                    "version": "0.4",
                }
            ]
        }

        # write metadata info into a multiscale scheme
        for axis, scale, offset, unit in zip(
            self.zarr_metadata["axes"],
            self.zarr_metadata["scale"],
            self.zarr_metadata["translation"],
            self.zarr_metadata["units"],
        ):
            multscale_dict["multiscales"][0]["axes"].append(
                {"name": axis, "type": "space", "unit": unit}
            )
            multscale_dict["multiscales"][0]["datasets"][0][
                "coordinateTransformations"
            ][0]["scale"].append(scale)
            multscale_dict["multiscales"][0]["datasets"][0][
                "coordinateTransformations"
            ][1]["translation"].append(offset)
        multscale_dict["multiscales"][0]["datasets"][0]["path"] = list(
            root.array_keys()
        )[0]

        # add multiscale template to .attrs
        root.attrs["multiscales"] = multscale_dict["multiscales"]


def write_volume_slab_to_zarr(chunk_num: int, zarray: zarr.Array, src_path: str):

    # check if the slab is at the array boundary or not
    if chunk_num + zarray.chunks[0] > zarray.shape[0]:
        slab_thickness = zarray.shape[0] - chunk_num
    else:
        slab_thickness = zarray.chunks[0]

    slab_shape = [slab_thickness] + list(zarray.shape[-2:])
    np_slab = np.empty(slab_shape, zarray.dtype)

    tiff_slab = imread(src_path, key=range(chunk_num, chunk_num + slab_thickness, 1))
    np_slab[0 : zarray.chunks[0], :, :] = tiff_slab

    # write a tiff stack slab into zarr array
    zarray[chunk_num : chunk_num + zarray.chunks[0], :, :] = np_slab
