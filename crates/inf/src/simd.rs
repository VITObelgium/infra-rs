use std::simd::{LaneCount, Simd, SimdCast, SupportedLaneCount};

pub trait SimdCastPl<const N: usize>
where
    LaneCount<N>: SupportedLaneCount,
{
    fn simd_cast<U: SimdCast>(self) -> Simd<U, N>;
}

macro_rules! impl_cast_custom {
    ($_type:ty, $_trait:ident) => {
        impl<const N: usize> SimdCastPl<N> for Simd<$_type, N>
        where
            std::simd::LaneCount<N>: SupportedLaneCount,
        {
            fn simd_cast<U: SimdCast>(self) -> Simd<U, N> {
                use std::simd::num::$_trait;
                self.cast::<U>()
            }
        }
    };
}

impl_cast_custom!(u8, SimdUint);
impl_cast_custom!(u16, SimdUint);
impl_cast_custom!(u32, SimdUint);
impl_cast_custom!(u64, SimdUint);
impl_cast_custom!(i8, SimdInt);
impl_cast_custom!(i16, SimdInt);
impl_cast_custom!(i32, SimdInt);
impl_cast_custom!(i64, SimdInt);
impl_cast_custom!(f32, SimdFloat);
impl_cast_custom!(f64, SimdFloat);
