This script could be used for conversion of a .tiff file to .zarr format with OME-NGFF multiscales metadata structure.
#### How to run
1. open command line terminal
2. install poetry tool for dependency management and packaging: https://pypi.org/project/poetry/
3. switch to the tif_to_zarr directory:
    ``cd PATH_TO_DIRECTORY/tif_to_zarr``
4. install python dependencies:
    ``poetry install``
5. run script using cli:
    ``poetry run python src/tif_to_zarr.py --src=PATH_TO_SOURCE_DIRECTORY/input_file.tif --dest=PATH_TO_DEST_DIRECTORY/output_file.zarr``
6. to convert a tiff file to zarr with custom metadata values, you can run smthg similar to this:
``poetry run python3 src/tif_to_zarr.py --src=path_to_source_tif  --dest=path_to_output_zarr -a x z y -t 1.0 2.0 3.0 -s 4.0 5.0 6.0 -u nanometer nanometer nanometer``
7. To get the list of options, you may run this:
``poetry run python3 src/tif_to_zarr.py --help``
