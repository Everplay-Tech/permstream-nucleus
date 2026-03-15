use crate::{compress_stream, decompress_stream, CodecConfig};
use std::io::Cursor;
use std::slice;

#[no_mangle]
pub extern "C" fn permstream_compress_c(
    in_buf: *const u8,
    in_size: usize,
    out_buf: *mut u8,
    out_size: *mut usize,
) -> i32 {
    let input = unsafe { slice::from_raw_parts(in_buf, in_size) };
    let mut out_vec = Vec::with_capacity(in_size); // Pre-allocate to minimize resizing
    let config = CodecConfig::default(); // Uses fast defaults
    
    if compress_stream(Cursor::new(input), &mut out_vec, config).is_ok() {
        let out_len = out_vec.len();
        unsafe {
            if out_len > *out_size { return -1; } // Output buffer too small
            std::ptr::copy_nonoverlapping(out_vec.as_ptr(), out_buf, out_len);
            *out_size = out_len;
        }
        0
    } else {
        -1
    }
}

#[no_mangle]
pub extern "C" fn permstream_decompress_c(
    in_buf: *const u8,
    in_size: usize,
    out_buf: *mut u8,
    out_size: *mut usize,
) -> i32 {
    let input = unsafe { slice::from_raw_parts(in_buf, in_size) };
    let mut out_vec = Vec::with_capacity(unsafe { *out_size }); 
    
    if decompress_stream(Cursor::new(input), &mut out_vec).is_ok() {
        let out_len = out_vec.len();
        unsafe {
            if out_len > *out_size { return -1; } // Output buffer too small
            std::ptr::copy_nonoverlapping(out_vec.as_ptr(), out_buf, out_len);
            *out_size = out_len;
        }
        0
    } else {
        -1
    }
}
