use std::u32;

pub fn uint32_unpack(data: &[u8]) -> u32 {
    assert!(data.len() >= 4);
    u32::from_le(unsafe { (*(data.as_ptr() as *const u32)) })
}

pub fn uint32_unpack2(buf: &[u8]) -> (u32, u32) {
    (uint32_unpack(&buf[0..4]), uint32_unpack(&buf[4..8]))
}

pub fn uint32_pack(data: &mut [u8], src: u32) {
    assert!(data.len() >= 4);
    unsafe { (*(data.as_mut_ptr() as *mut u32)) = u32::to_le(src) };
}

pub fn uint32_pack2(data: &mut [u8], src0: u32, src1: u32) {
    assert!(data.len() >= 8);
    uint32_pack(&mut data[0..4], src0);
    uint32_pack(&mut data[4..8], src1);
}
