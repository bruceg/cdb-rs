use std::u32;

pub fn uint32_unpack(data: &[u8]) -> u32 {
    assert!(data.len() >= 4);
    u32::from_le(unsafe { (*(data.as_ptr() as *const u32)) })
}

pub fn uint32_pack(data: &mut [u8], src: u32) {
    assert!(data.len() >= 4);
    unsafe { (*(data.as_ptr() as *mut u32)) = u32::to_le(src) };
}
