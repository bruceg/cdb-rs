extern crate cdb;

#[test]
fn test_one() {
    let cdb = cdb::CDB::open("tests/test1.cdb").unwrap();
    let mut i = cdb.find(b"one");
    assert_eq!(i.next().unwrap().unwrap(), b"Hello");
    assert_eq!(i.next().unwrap().unwrap(), b", World!");
}


#[test]
fn test_two() {
    let cdb = cdb::CDB::open("tests/test1.cdb").unwrap();
    assert_eq!(cdb.find(b"two").next().unwrap().unwrap(), b"Goodbye");
    assert_eq!(cdb.find(b"this key will be split across two reads").next().unwrap().unwrap(), b"Got it.");
}
