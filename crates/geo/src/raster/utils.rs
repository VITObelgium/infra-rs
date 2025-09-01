use std::mem::MaybeUninit;

pub fn reinterpret_uninit_byte_slice<T>(data: &mut [MaybeUninit<u8>]) -> &mut [MaybeUninit<T>] {
    debug_assert_eq!(data.len() % std::mem::size_of::<T>(), 0);
    unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr().cast::<MaybeUninit<T>>(), data.len() / std::mem::size_of::<T>()) }
}

pub fn reinterpret_uninit_slice_to_byte<T>(data: &mut [MaybeUninit<T>]) -> &mut [MaybeUninit<u8>] {
    unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr().cast::<MaybeUninit<u8>>(), data.len() * std::mem::size_of::<T>()) }
}
