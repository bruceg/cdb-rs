use std::u32;

pub fn unpack(data: &[u8]) -> u32 {
    assert!(data.len() >= 4);
    // Use u32::from_bytes when it stabilizes
    // Rust compiles this down to an efficient word copy
    (data[0] as u32) | ((data[1] as u32) << 8) | ((data[2] as u32) << 16) | ((data[3] as u32) << 24)
}

pub fn unpack2(buf: &[u8]) -> (u32, u32) {
    (unpack(&buf[0..4]), unpack(&buf[4..8]))
}

fn _pack(src: u32) -> [u8; 4] {
    // Use u32::to_bytes when it stabilizes
    // Rust compiles this down to an efficient word copy
    [
        src as u8,
        (src >> 8) as u8,
        (src >> 16) as u8,
        (src >> 24) as u8,
    ]
}

pub fn pack(data: &mut [u8], src: u32) {
    assert!(data.len() >= 4);
    data[..4].copy_from_slice(&_pack(src));
}

pub fn pack2(data: &mut [u8], src0: u32, src1: u32) {
    assert!(data.len() >= 8);
    pack(&mut data[0..4], src0);
    pack(&mut data[4..8], src1);
}
