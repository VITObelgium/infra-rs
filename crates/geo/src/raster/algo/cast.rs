use crate::{Array, ArrayNum};
use num::NumCast;

pub fn cast<TDest, R>(src: &R) -> R::WithPixelType<TDest>
where
    R: Array,
    TDest: ArrayNum,
    for<'a> &'a R: IntoIterator<Item = Option<R::Pixel>>,
{
    R::WithPixelType::<TDest>::from_iter(src.metadata().clone(), src.into_iter().map(|x| x.and_then(|x| NumCast::from(x))))
        .expect("Raster size bug") // Can only fail if the metadata size is invalid which is impossible in this case
}
