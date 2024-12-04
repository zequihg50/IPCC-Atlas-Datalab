import sys
import numpy as np
import netCDF4

def copy_attrs(v1, v2):
    for attr in v1.ncattrs():
        if attr.startswith("_"):
            continue
        v2.setncattr(attr, v1.getncattr(attr))

class FileProcessor:
    def __init__(self, src, dst):
        self._src = src
        self._dst = dst

    def process(self):
        self.copy_global_attrs()
        self.create_dimensions()
        self.create_variables()
        self.create_climate_variables()

    def copy_global_attrs(self):
        ignore = ["contact", "title", "project", "product", "product_version", "date_created", "tracking_id", "time_min", "time_max"]
        for attr in self._src.ncattrs():
            if attr in ignore or attr.startswith("_") or attr.startswith("geospatial_"):
                continue
            self._dst.setncattr(attr, self._src.getncattr(attr))

    def create_dimensions(self):
        self._dst.createDimension("member", self._src.dimensions["member"].size)
        self._dst.createDimension("time", self._src.dimensions["time"].size)
        self._dst.createDimension("lat", self._src.dimensions["lat"].size)
        self._dst.createDimension("lon", self._src.dimensions["lon"].size)
        self._dst.createDimension("bnds", 2)

    def create_variables(self):
        self._dst.createVariable("member", str, ("member",))
        copy_attrs(self._src["member_id"], self._dst["member"])
        self._dst["member"].setncattr("_CoordinateAxisType", b"Ensemble")
        self._dst["member"][...] = self._src["member_id"][...]

        self._dst.createVariable("time", self._src["time"].dtype, ("time",), chunksizes=(self._src["time"].shape[0],), compression="zlib", complevel=1, shuffle=True, fletcher32=True, fill_value=np.array(1.e20, dtype=self._src["time"].dtype))
        copy_attrs(self._src["time"], self._dst["time"])
        self._dst["time"][...] = self._src["time"][...]

        self._dst.createVariable("lat", self._src["lat"].dtype, ("lat",), chunksizes=(self._src["lat"].shape[0],), compression="zlib", complevel=1, shuffle=True, fletcher32=True, fill_value=np.array(1.e20, dtype=self._src["lat"].dtype))
        copy_attrs(self._src["lat"], self._dst["lat"])
        self._dst["lat"][...] = self._src["lat"][...]

        self._dst.createVariable("lon", self._src["lon"].dtype, ("lon",), chunksizes=(self._src["lon"].shape[0],), compression="zlib", complevel=1, shuffle=True, fletcher32=True, fill_value=np.array(1.e20, dtype=self._src["lon"].dtype))
        copy_attrs(self._src["lon"], self._dst["lon"])
        self._dst["lon"][...] = self._src["lon"][...]

        self._dst.createVariable("time_bnds", self._src["time_bnds"].dtype, ("time","bnds"), chunksizes=self._src["time_bnds"].shape, compression="zlib", complevel=1, shuffle=True, fletcher32=True, fill_value=np.array(1.e20, dtype=self._src["time_bnds"].dtype))
        copy_attrs(self._src["time_bnds"], self._dst["time_bnds"])
        self._dst["time_bnds"][...] = self._src["time_bnds"][...]

        self._dst.createVariable("lat_bnds", self._src["lat_bnds"].dtype, ("lat","bnds"), chunksizes=self._src["lat_bnds"].shape, compression="zlib", complevel=1, shuffle=True, fletcher32=True, fill_value=np.array(1.e20, dtype=self._src["lat_bnds"].dtype))
        copy_attrs(self._src["lat_bnds"], self._dst["lat_bnds"])
        self._dst["lat_bnds"][...] = self._src["lat_bnds"][...]

        self._dst.createVariable("lon_bnds", self._src["lon_bnds"].dtype, ("lon","bnds"), chunksizes=self._src["lon_bnds"].shape, compression="zlib", complevel=1, shuffle=True, fletcher32=True, fill_value=np.array(1.e20, dtype=self._src["lon_bnds"].dtype))
        copy_attrs(self._src["lon_bnds"], self._dst["lon_bnds"])
        self._dst["lon_bnds"][...] = self._src["lon_bnds"][...]

    def create_climate_variables(self):
        for v in self._src.variables:
            if v.startswith("gcm_") or v in self._dst.variables or v == "member_id" or v.startswith("rcm_"):
                continue
            elif len(self._src[v].shape) < 4:
                self._dst.createVariable(v, self._src[v].dtype, self._src[v].dimensions, chunksizes=self._src[v].shape, compression="zlib", complevel=1, shuffle=True, fletcher32=True)
                self._dst[v][...] = self._src[v][...]
                copy_attrs(self._src[v], self._dst[v])
            else:
                self._dst.createVariable(v, self._src[v].dtype, self._src[v].dimensions, chunksizes=(1,1,self._src.dimensions["lat"].size,self._src.dimensions["lon"].size), compression="zlib", complevel=9, shuffle=True, fletcher32=True, fill_value=np.array(1.e20, dtype=self._src[v].dtype))
                copy_attrs(self._src[v], self._dst[v])
                self._dst[v].setncattr("missing_value", np.array(1.e20, dtype=self._src[v].dtype))
                self._dst[v].setncattr("coordinates", "member time lat lon")
                if "height2m" in self._src.variables:
                    self._dst[v].setncattr("coordinates", "member time lat lon height2m")
                for i in range(self._dst.dimensions["member"].size):
                    try:
                        arr = self._src[v][i]
                        arr[arr == self._src[v].getncattr("missing_value")] = self._dst[v].getncattr("missing_value")
                        arr[arr == self._src[v].getncattr("_FillValue")] = self._dst[v].getncattr("missing_value")
                        self._dst[v][i] = arr
                    except:
                        print(f"Error on file {self._src.filepath()}, variable {v}, member {i}. Filling with missing_value.", file=sys.stderr)
                        self._dst[v][i] = self._dst[v].getncattr("missing_value")


if __name__ == "__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    print(f"Input: {input_file}, Output: {output_file}")

    src = netCDF4.Dataset(input_file, "r")
    f = netCDF4.Dataset(output_file, "w")

    processor = FileProcessor(src, f)
    processor.process()

    # close and exit
    src.close()
    f.close()
