"""This module contains the Conversion class used to convert tiff file to OME-NGFF Zarr (contains chunked array)."""

import dask.array as da
import numpy as np
from tifffile import imwrite, TiffFile
import zarr
import sys
import logging
from numcodecs import Zstd
from typing import List


class Conversion:
    """A class used to convert tiff file to zarr array with properly formatted OME-NGFF metadata."""

    def __init__(
        self,
        input_filepath: str,
        output_filepath: str,
        axes: List[str],
        translation: List[float],
        scale: List[float],
        units: List[str],
    ):
        """Construct all the necessary attributes for the proper conversion of tiff to OME-NGFF Zarr.

        Args:
            input_filepath (str): path to source tiff file
            output_filepath (str): path to the output zarr file.
            axes (List[str]): list of axes to store in metadata. Order matters.
            translation (List[float]): list of coordinates where the top left corner of the output zarr array should be located when displayed in neuroglancer. Order matters
            scale (List[float]): physical size of the voxel (in units). Order matters.
            units (List[str]): physical dimension units that define in which units the scale attribute is measured. Order matters.
        """
        self.input_filepath = input_filepath
        self.output_filepath = output_filepath
        self.zarr_metadata = {
            "axes": axes,
            "translation": translation,
            "scale": scale,
            "units": units,
        }

    def read_tiff(self):
        """Read tiff file and store array, axes and metadata in a dictionary.

        Returns:
            [numpy.array, [str], dict]: returns tiff image as numpy array object, axis naming and order, and imagej style metadata.
        """
        try:
            with TiffFile(self.input_filepath) as tiff:
                volume_numpy = tiff.asarray()
                # volume_dask = da.from_array(tiff.asarray(), chunks=chunks)
                axes = tiff.series[0].axes
                imagej_metadata = tiff.imagej_metadata
        except IOError as e:
            logging.error(
                "Failed to open {0}. Error reason: {1}".format(self.input_filepath, e)
            )
            sys.exit(1)
        return [volume_numpy, axes, imagej_metadata]

    def dask_to_zarray(self, tiff_data: List, chunks: List):
        """Store dask array in a zarr file.

        Args:
            tiff_data (List): a list containing tiff image as numpy array object, axis naming and order, and imagej style metadata.
            chunks (List): what chunk size to use for output zarr array
        """
        # create root group
        root = zarr.group(
            store=zarr.NestedDirectoryStore(self.output_filepath), overwrite=True
        )
        # create zarr array
        # zarr_data = root.create_dataset('data', shape=tiff_data[0].shape, chunks=chunks, dtype=tiff_data[0].dtype)
        dask_arr = da.from_array(tiff_data[0], chunks=chunks)
        zarr_data = zarr.create(
            store=zarr.NestedDirectoryStore(self.output_filepath),
            path="s0",
            shape=dask_arr.shape,
            chunks=chunks,
            dtype=dask_arr.dtype,
            compressor=Zstd(level=6),
        )

        # store .tiff data in a .zarr file
        da.store(dask_arr, zarr_data)

        # add metadata to zarr .attrs.
        self.populate_zarr_attrs(
            root, tiff_data[1], self.zarr_metadata, zarr_data.name.lstrip("/")
        )

    def numpy_to_zarray(self, tiff_data, chunks):
        """Store numpy array in a zarr file.

        Args:
            tiff_data (List): a list containing tiff image as numpy array object, axis naming and order, and imagej style metadata.
            chunks (List): what chunk size to use for output zarr array
        """
        # create root group
        root = zarr.group(
            store=zarr.NestedDirectoryStore(self.output_filepath), overwrite=True
        )
        # create zarr array
        zarr_data = zarr.create(
            store=zarr.NestedDirectoryStore(self.output_filepath),
            path="s0",
            shape=tiff_data[0].shape,
            chunks=chunks,
            dtype=tiff_data[0].dtype,
        )
        zarr_data[:] = tiff_data[0]

        self.populate_zarr_attrs(
            root, tiff_data[1], self.zarr_metadata, zarr_data.name.lstrip("/")
        )

    def populate_zarr_attrs(self, root, axes, zarr_metadata, data_address):
        """Add selected tiff metadata to zarr attributes file (.zattrs).

        Args:
            root (zarr.Group): root group of the output zarr array
            axes (List): axes naming order (z,y,x or x,y,z)
            zarr_metadata (): combined zarr metadata from input translation, scale, axes names and units
            data_address (str): path to array
        """
        tiff_axes = [*axes]

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
                    "name": "unknown",
                    "version": "0.4",
                }
            ]
        }

        # write metadata info into a multiscale scheme
        for axis, scale, offset, unit in zip(
            zarr_metadata["axes"],
            zarr_metadata["scale"],
            zarr_metadata["translation"],
            zarr_metadata["units"],
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
        multscale_dict["multiscales"][0]["datasets"][0]["path"] = data_address  ##

        # add multiscale template to .attrs
        root.attrs["multiscales"] = multscale_dict["multiscales"]
