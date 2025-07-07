#[cfg(feature = "simd")]
pub type AlignedVec<T> = Vec<T, allocator::CacheAligned>;
#[cfg(not(feature = "simd"))]
pub type AlignedVec<T> = Vec<T>;

/// Create an empty aligned vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn new_aligned_vec<T>() -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    return Vec::new_in(allocator::CacheAligned);

    #[cfg(not(feature = "simd"))]
    return Vec::new();
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_with_capacity<T>(capacity: usize) -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    return Vec::with_capacity_in(capacity, allocator::CacheAligned);

    #[cfg(not(feature = "simd"))]
    return Vec::with_capacity(capacity);
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_from_iter<T, I: IntoIterator<Item = T>>(iter: I) -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    {
        let mut vec = new_aligned_vec();
        iter.into_iter().for_each(|item| vec.push(item));
        vec
    }

    #[cfg(not(feature = "simd"))]
    return Vec::from_iter(iter);
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_from_slice<T: Copy>(slice: &[T]) -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    return slice.to_vec_in(allocator::CacheAligned);

    #[cfg(not(feature = "simd"))]
    return slice.to_vec();
}

/// Create a vec with the buffer aligned to a cache line for simd usage if the simd feature is enabled.
/// Otherwise a regular Vec is created.
pub fn aligned_vec_filled_with<T: Copy>(val: T, len: usize) -> AlignedVec<T> {
    #[cfg(feature = "simd")]
    {
        let mut vec = aligned_vec_with_capacity(len);
        vec.resize(len, val);
        vec
    }

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
        let (ptr, len, cap, alloc) = data.into_raw_parts_with_alloc();

        unsafe { Vec::from_raw_parts_in(ptr.cast::<TDest>(), len, cap, alloc) }
    }

    #[cfg(not(feature = "simd"))]
    {
        let ptr = data.as_ptr() as *mut TDest;
        let len = data.len();
        let cap = data.capacity();
        #[allow(clippy::mem_forget)] // No longer needed when Vec::into_raw_parts is stable
        std::mem::forget(data); // Avoid dropping the original Vec

        unsafe { Vec::from_raw_parts(ptr, len, cap) }
    }
}

#[cfg(feature = "simd")]
pub mod allocator {
    use std::alloc::{AllocError, Layout};

    const CACHELINE_ALIGN: usize = 64; // Common cache line size

    #[derive(Clone)]
    pub struct CacheAligned;

    unsafe impl std::alloc::Allocator for CacheAligned {
        fn allocate(&self, layout: Layout) -> Result<std::ptr::NonNull<[u8]>, AllocError> {
            let aligned_layout = Layout::from_size_align(layout.size(), CACHELINE_ALIGN).map_err(|_| AllocError)?;

            let ptr = unsafe { std::alloc::alloc(aligned_layout) };

            let ptr = std::ptr::NonNull::new(ptr).ok_or(std::alloc::AllocError)?;
            // SAFETY: we just allocated `layout.size()` bytes.
            Ok(std::ptr::NonNull::slice_from_raw_parts(ptr, layout.size()))
        }

        unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
            let aligned_layout = Layout::from_size_align(layout.size(), CACHELINE_ALIGN).expect("Invalid layout for cache line alignment");
            unsafe { std::alloc::dealloc(ptr.as_ptr(), aligned_layout) };
        }
    }
}
