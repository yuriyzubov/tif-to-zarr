This script could be used for conversion of a .tiff file to .zarr format with OME-NGFF multiscales metadata structure.
#### How to run
1. open command line terminal
2. install poetry tool for dependency management and packaging: https://pypi.org/project/poetry/
3. switch to the tif_to_zarr directory:
    ``cd PATH_TO_DIRECTORY/tif_to_zarr``
4. install python dependencies:
    ``poetry install``
5. run script using cli:
    ``poetry run python src/tif_to_zarr.py --src=PATH_TO_SOURCE_DIRECTORY/input_file.n5 --dest=PATH_TO_DEST_DIRECTORY/output_file.zarr``
