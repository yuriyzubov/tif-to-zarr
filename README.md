This script could be used for conversion of a .tiff file to .zarr format with OME-NGFF multiscales metadata structure.
#### How to run
1. open command line terminal
2. install tif_to_zarr
    ``pip install tif_to_zarr``
5. run script using cli:
    ``tif_to_zarr --src=PATH_TO_SOURCE_DIRECTORY/input_file.tif --dest=PATH_TO_DEST_DIRECTORY/output_file.zarr``
6. to convert a tiff file to zarr with custom metadata values, you can run smthg similar to this:
``tif_to_zarr --src=path_to_source_tif  --dest=path_to_output_zarr -a x z y -t 1.0 2.0 3.0 -s 4.0 5.0 6.0 -u nanometer nanometer nanometer``
7. To get the list of options, you may run this:
``tif_to_zarr --help``
