#[cfg(feature = "simd")]
pub type AlignedVec<T> = aligned_vec::AVec<T>;
#[cfg(not(feature = "simd"))]
pub type AlignedVec<T> = Vec<T>;

/// Create an empty aligned vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn new_aligned_vec<T>() -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    return aligned_vec::AVec::<T>::new(aligned_vec::CACHELINE_ALIGN);

    #[cfg(not(feature = "simd"))]
    return Vec::new();
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_with_capacity<T>(capacity: usize) -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    return aligned_vec::AVec::<T>::with_capacity(aligned_vec::CACHELINE_ALIGN, capacity);

    #[cfg(not(feature = "simd"))]
    return Vec::with_capacity(capacity);
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_from_iter<T, I: IntoIterator<Item = T>>(iter: I) -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    return aligned_vec::AVec::<T>::from_iter(aligned_vec::CACHELINE_ALIGN, iter);

    #[cfg(not(feature = "simd"))]
    return Vec::from_iter(iter);
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_from_slice<T: Copy>(slice: &[T]) -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    return aligned_vec::AVec::<T>::from_slice(aligned_vec::CACHELINE_ALIGN, slice);

    #[cfg(not(feature = "simd"))]
    return slice.to_vec();
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_filled_with<T: Copy>(val: T, len: usize) -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    return aligned_vec::avec![val; len];

    #[cfg(not(feature = "simd"))]
    return vec![val; len];
}

/// # Safety
///
/// `T` and `TDest` must have the same size.
pub unsafe fn reinterpret_aligned_vec<T: Sized, TDest: Sized>(data: AlignedVec<T>) -> AlignedVec<TDest> {
    assert!(
        std::mem::size_of::<T>() == std::mem::size_of::<TDest>(),
        "Cannot reinterpret AlignedVec<T> to AlignedVec<TDest> because their sizes do not match"
    );

    #[cfg(feature = "simd")]
    {
        let (ptr, align, len, cap) = data.into_raw_parts();

        unsafe { aligned_vec::AVec::from_raw_parts(ptr.cast::<TDest>(), align, len, cap) }
    }

    #[cfg(not(feature = "simd"))]
    {
        let ptr = data.as_ptr() as *mut TDest;
        let len = data.len();
        let cap = data.capacity();
        std::mem::forget(data); // Avoid dropping the original Vec

        unsafe { Vec::from_raw_parts(ptr, len, cap) }
    }
}
