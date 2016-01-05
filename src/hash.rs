const HASHSTART: u32 = 0x1505;

pub fn add(h: u32, c: u8) -> u32 {
    //(h + (h << 5)) ^ (c as u32)
    h.wrapping_shl(5).wrapping_add(h) ^ (c as u32)
}

pub fn hash(buf: &[u8]) -> u32 {
    let mut h = HASHSTART;
    for c in buf {
        h = add(h, *c);
    }
    h
}

#[test]
fn samples() {
    assert_eq!(hash(b""), 0x0001505);
    assert_eq!(hash(b"Hello, world!"), 0x564369e8);
    assert_eq!(hash(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 0x40032705);
}
