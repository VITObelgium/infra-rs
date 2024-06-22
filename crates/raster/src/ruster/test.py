import ruster
import logging
import pyarrow as pa
import pyarrow.compute as pc
import numpy as np

FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.INFO)

ras = ruster.read_raster('/Users/dirk/Projects/rs-infra/test/data/landusebyte.tif')
print(ras)
print(ras.meta_data)
print(pc.min_max(ras.arrow_data))
print("------------------------")
ras = ruster.read_raster_as('f32', '/Users/dirk/Projects/rs-infra/test/data/landusebyte.tif')
print(ras)
print(ras.meta_data)
print("------------------------")
ras = ruster.read_raster_as(pa.int32(), '/Users/dirk/Projects/rs-infra/test/data/landusebyte.tif')
print(ras)
print(ras.meta_data)
print("------------------------")
ras = ruster.read_raster_as(np.dtype('u4'), '/Users/dirk/Projects/rs-infra/test/data/landusebyte.tif')
print(ras)
print(ras.meta_data)
