import dask.array as da
import numpy as np
from tifffile import imwrite, TiffFile
import zarr
import sys
import logging
import dask

class Conversion:
    def __init__(self, input_filepath, output_filepath, axes, translation, scale, units):
        self.input_filepath = input_filepath
        self.output_filepath = output_filepath
        self.zarr_metadata = {"axes" : axes, "translation" : translation, "scale" : scale, "units" : units}

    #read tiff file and store array, axes and metadata in a dictionary. 
    def read_tiff(self):
        try:
            with TiffFile(self.input_filepath) as tiff:
                volume_numpy = tiff.asarray()
                #volume_dask = da.from_array(tiff.asarray(), chunks=chunks)
                axes = tiff.series[0].axes
                imagej_metadata = tiff.imagej_metadata
        except IOError as e:
            logging.error("Failed to open {0}. Error reason: {1}".format(self.input_filepath, e))
            sys.exit(1)
        return [volume_numpy, axes, imagej_metadata]

    # store numpy array in a zarr file
    def dask_to_zarray(self, tiff_data, chunks):

        #create root group 
        root = zarr.group(store = zarr.NestedDirectoryStore(self.output_filepath), overwrite=True)
        # create zarr array
        #zarr_data = root.create_dataset('data', shape=tiff_data[0].shape, chunks=chunks, dtype=tiff_data[0].dtype)
        dask_arr = da.from_array(tiff_data[0], chunks=chunks)
        zarr_data = zarr.create(store=zarr.NestedDirectoryStore(self.output_filepath), path='s0', shape = dask_arr.shape, chunks=chunks, dtype=dask_arr.dtype)

        #store .tiff data in a .zarr file 
        da.store(dask_arr, zarr_data)

        # add metadata to zarr .attrs. 
        self.populate_zarr_attrs(root, tiff_data[1], self.zarr_metadata, zarr_data.name.lstrip('/'))


    def numpy_to_zarray(self, tiff_data, chunks):
        #create root group 
        root = zarr.group(store = zarr.NestedDirectoryStore(self.output_filepath), overwrite=True)
        # create zarr array
        zarr_data = zarr.create(store=zarr.NestedDirectoryStore(self.output_filepath), 
                                path='s0', 
                                shape = tiff_data[0].shape,
                                chunks=chunks,
                                dtype=tiff_data[0].dtype)
        zarr_data[:] = tiff_data[0]


        self.populate_zarr_attrs(root, tiff_data[1], self.zarr_metadata, zarr_data.name.lstrip('/'))

        
    # add selected tiff metadata to zarr .attrs  
    def populate_zarr_attrs(self, root, axes, zarr_metadata, data_address):
        tiff_axes=[*axes]

        # json template for a multiscale structure
        multscale_dict = {
        "multiscales": [
            {
                "axes": [
                    
                ],
                "datasets": [
                    {
                        "coordinateTransformations": [
                            {
                            "scale": [
                                
                            ],
                            "type": "scale"
                        },
                        {
                            "translation": [
                                
                            ],
                            "type": "translation"
                        }
                        ],
                        "path": "unknown"
                    }
                ],
                "name": "unknown",
                "version": "0.4"
            }
        ]
        }

        # write metadata info into a multiscale scheme 
        for axis, scale, offset, unit in zip(zarr_metadata["axes"], zarr_metadata["scale"], zarr_metadata["translation"], zarr_metadata["units"]):
            multscale_dict["multiscales"][0]["axes"].append( {
                            "name": axis,
                            "type": "space",
                            "unit" : unit
                        })
            multscale_dict["multiscales"][0]["datasets"][0]["coordinateTransformations"][0]["scale"].append(scale)
            multscale_dict["multiscales"][0]["datasets"][0]["coordinateTransformations"][1]["translation"].append(offset)
        multscale_dict["multiscales"][0]["datasets"][0]["path"] = data_address ##

        # add multiscale template to .attrs 
        root.attrs["multiscales"] = multscale_dict["multiscales"]

            