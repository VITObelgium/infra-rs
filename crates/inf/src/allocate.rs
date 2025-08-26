use std::mem::MaybeUninit;

#[cfg(feature = "simd")]
pub type AlignedVec<T> = Vec<T, allocator::CacheAligned>;
#[cfg(not(feature = "simd"))]
pub type AlignedVec<T> = Vec<T>;

/// Helper struct to create an aligned vec while avoiding unnecessary memory initializaton while constructing.
pub struct AlignedVecUnderConstruction<T: bytemuck::AnyBitPattern> {
    vec: AlignedVec<MaybeUninit<T>>,
}

impl<T: bytemuck::AnyBitPattern> AlignedVecUnderConstruction<T> {
    /// Create a new aligned vec under construction with the given length.
    pub fn new(len: usize) -> Self {
        Self {
            vec: aligned_vec_uninit(len),
        }
    }

    pub fn from_vec(vec: AlignedVec<T>) -> Self
    where
        T: bytemuck::NoUninit,
        MaybeUninit<T>: bytemuck::AnyBitPattern,
    {
        Self {
            vec: cast_aligned_vec(vec),
        }
    }

    /// Obtain the underlying buffer as a mutable byte slice
    /// # Safety
    /// The caller must ensure that the buffer is used correctly and that the data is not accessed in an invalid way.
    pub unsafe fn as_byte_slice_mut(&mut self) -> &mut [u8] {
        unsafe {
            // Safety: The buffer is allocated with enough capacity to hold the decoded data
            std::slice::from_raw_parts_mut(self.vec.as_mut_ptr().cast::<u8>(), self.vec.capacity() * std::mem::size_of::<T>())
        }
    }

    /// Obtain the underlying buffer as a mutable byte slice
    /// # Safety
    /// The caller must ensure that the buffer is used correctly and that the data is not accessed in an invalid way.
    pub unsafe fn as_slice_mut(&mut self) -> &mut [T] {
        unsafe {
            // Safety: The buffer is allocated with enough capacity to hold the decoded data
            std::slice::from_raw_parts_mut(self.vec.as_mut_ptr().cast::<T>(), self.vec.capacity())
        }
    }

    /// Obtain the underlying buffer as a mutable slice of `MaybeUninit<T>`.
    /// This allows the caller to initialize the elements of the vec.
    pub fn as_uninit_slice_mut(&mut self) -> &mut [MaybeUninit<T>] {
        &mut self.vec
    }

    /// Obtain the underlying buffer as a mutable slice of `MaybeUninit<u8>`.
    /// This allows the caller to initialize the elements of the vec.
    pub fn as_uninit_byte_slice_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.vec.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                self.vec.capacity() * std::mem::size_of::<T>(),
            )
        }
    }

    /// Convert the aligned vec under construction to an aligned vec of initialized elements.
    /// # Safety
    /// The caller must ensure that all elements of the `AlignedVec<MaybeUninit<T>>` are initialized before calling this function.
    pub unsafe fn assume_init(self) -> AlignedVec<T> {
        unsafe { aligned_vec_assume_init(self.vec) }
    }
}

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
/// Otherwise a regular Vec is created. The vec is of of the requested size filled with `MaybeUninit<T>`
/// This avoids the cost of initializing the elements, callers will have to initialize the elements themselves.
/// and convert them to `T` when they are ready.
pub fn aligned_vec_uninit<T>(len: usize) -> AlignedVec<MaybeUninit<T>> {
    #[cfg(feature = "simd")]
    let mut vec = Vec::with_capacity_in(len, allocator::CacheAligned);

    #[cfg(not(feature = "simd"))]
    let mut vec = Vec::with_capacity(len);

    unsafe {
        vec.set_len(len);
    }

    vec
}

/// # Safety
///
/// The caller must ensure that all elements of the `AlignedVec<MaybeUninit<T>>` are initialized before calling this function.
pub unsafe fn aligned_vec_assume_init<T>(vec: AlignedVec<MaybeUninit<T>>) -> AlignedVec<T> {
    unsafe { std::mem::transmute::<AlignedVec<MaybeUninit<T>>, AlignedVec<T>>(vec) }
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
pub fn cast_aligned_vec<T: bytemuck::NoUninit, TDest: bytemuck::AnyBitPattern>(data: AlignedVec<T>) -> AlignedVec<TDest> {
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
    bytemuck::cast_vec(data)
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
