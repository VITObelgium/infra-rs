#[cfg(feature = "simd")]
fn avec_to_vec<T, A: aligned_vec::Alignment>(avec: aligned_vec::AVec<T, A>) -> Vec<T> {
    let (ptr, _align, len, cap) = avec.into_raw_parts();

    // SAFETY: We just move the pointer from the AVec to the Vec instance.
    unsafe { Vec::from_raw_parts(ptr, len, cap) }
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_with_capacity<T>(capacity: usize) -> Vec<T> {
    #[cfg(feature = "simd")]
    return avec_to_vec(aligned_vec::AVec::<T>::with_capacity(aligned_vec::CACHELINE_ALIGN, capacity));

    #[cfg(not(feature = "simd"))]
    return Vec::with_capacity(capacity);
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_filled_with<T: Copy>(val: T, len: usize) -> Vec<T> {
    #[cfg(feature = "simd")]
    return avec_to_vec(aligned_vec::avec![val; len]);

    #[cfg(not(feature = "simd"))]
    return vec![val; len];
}
