use std::simd::{LaneCount, Simd, SimdCast, SupportedLaneCount};

#[cfg(all(target_arch = "wasm32", target_feature = "simd128"))]
pub const LANES: usize = 4; // wasm SIMD128 (4 x f32 lanes)

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
pub const LANES: usize = 16; // AVX-512 512-bit (16 x f32 lanes)

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512f")))]
pub const LANES: usize = 8; // AVX2 256-bit (8 x f32 lanes)

#[cfg(all(target_arch = "x86_64", target_feature = "sse2", not(target_feature = "avx2")))]
pub const LANES: usize = 4; // SSE2 128-bit (4 x f32 lanes)

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub const LANES: usize = 4; // NEON 128-bit (4 x f32 lanes)

// Fallback if none of the above matches
#[cfg(not(any(
    all(target_arch = "wasm32", target_feature = "simd128"),
    all(target_arch = "x86_64", target_feature = "avx512f"),
    all(target_arch = "x86_64", target_feature = "avx2"),
    all(target_arch = "x86_64", target_feature = "sse2"),
    all(target_arch = "aarch64", target_feature = "neon")
)))]
pub const LANES: usize = 1; // scalar fallback

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
