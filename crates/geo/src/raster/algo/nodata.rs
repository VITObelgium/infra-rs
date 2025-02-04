use crate::Array;
use crate::Nodata;

pub fn replace_nodata_in_place<RasterType>(ras: &mut RasterType, new_value: RasterType::Pixel)
where
    RasterType: Array,
{
    ras.iter_mut().for_each(|x| {
        if x.is_nodata() {
            *x = new_value;
        }
    });
}

pub fn replace_nodata<RasterType>(ras: &RasterType, new_value: RasterType::Pixel) -> RasterType
where
    RasterType: Array,
{
    let mut result = ras.clone();
    replace_nodata_in_place(&mut result, new_value);
    result
}
