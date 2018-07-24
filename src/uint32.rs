use std::u32;

pub fn unpack(data: &[u8]) -> u32 {
    assert!(data.len() >= 4);
    u32::from_le(unsafe { (*(data.as_ptr() as *const u32)) })
}

pub fn unpack2(buf: &[u8]) -> (u32, u32) {
    (unpack(&buf[0..4]), unpack(&buf[4..8]))
}

pub fn pack(data: &mut [u8], src: u32) {
    assert!(data.len() >= 4);
    unsafe { (*(data.as_mut_ptr() as *mut u32)) = u32::to_le(src) };
}

pub fn pack2(data: &mut [u8], src0: u32, src1: u32) {
    assert!(data.len() >= 8);
    pack(&mut data[0..4], src0);
    pack(&mut data[4..8], src1);
}
