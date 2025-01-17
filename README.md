This script could be used for conversion of a .tiff file to .zarr format with OME-NGFF multiscales metadata structure.
#### How to run
1. open command line terminal
2. install tif_to_zarr
    ``pip install tiff-to-zarr``
5. run script using cli:
    ``tiff-to-zarr --src=PATH_TO_SOURCE_DIRECTORY/input_file.tif --dest=PATH_TO_DEST_DIRECTORY/output_file.zarr --num_workers=20, --cluster=local``
6. to convert a tiff file to zarr with custom metadata values, you can run smthg similar to this:
``tiff-to-zarr --src=path_to_source_tif(3d or stack)  --dest=path_to_output_zarr --num_workers=20 --cluster=local(or lsf) --zarr_chunks 13 128 128 --axes x z y --scale 4.0 5.0 6.0 --translation 1.0 2.0 3.0 --units nanometer nanometer nanometer``
7. To get the list of options, you may run this:
``tiff-to-zarr --help``
