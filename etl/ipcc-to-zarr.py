import sys
import numpy as np
import xarray
import dask
import zarr
import h5py

if __name__ == "__main__":
    dask.config.set(scheduler="processes")
    src = sys.argv[1]
    out = sys.argv[2]

    # extract chunking
    chunks = {}
    with h5py.File(src) as f:
        for v in list(f):
            if f[v].chunks:
                chunks[v] = f[v].chunks
            else:
                chunks[v] = {}

    ds = xarray.open_dataset(src, decode_cf=False, decode_timedelta=False)
    for v in chunks:
        ds[v] = ds[v].chunk(chunks[v])
    ds["time_bnds"] = ds["time_bnds"].chunk(time=ds["time"].size)

    compressor = zarr.Blosc(cname='zlib', clevel=9, shuffle=1)
    encoding = dict()
    for v in ds.variables:
        encoding[v] = { "compressor": compressor }
        #if len(ds[v].shape) >= 1:
        #    encoding[v]["fill_value"] = np.array(1.e20, dtype=ds[v].dtype)

    ds.to_zarr(out, encoding=encoding)

