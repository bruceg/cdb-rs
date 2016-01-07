extern crate cdb;
use std::fs;

#[test]
fn test_one() {
    //let test1[u8] = include_bytes!("tests/test1.cdb");
    let f = fs::File::open("tests/test1.cdb").unwrap();
    let mut cdb = cdb::CDB::init(f).unwrap();
    let mut i = cdb.find(b"one");
    assert_eq!(i.next().unwrap().unwrap(), b"Hello");
    assert_eq!(i.next().unwrap().unwrap(), b", World!");
}


#[test]
fn test_two() {
    //let test1[u8] = include_bytes!("tests/test1.cdb");
    let f = fs::File::open("tests/test1.cdb").unwrap();
    let mut cdb = cdb::CDB::init(f).unwrap();
    assert_eq!(cdb.find(b"two").next().unwrap().unwrap(), b"Goodbye");
    assert_eq!(cdb.find(b"this key will be split across two reads").next().unwrap().unwrap(), b"Got it.");
}
