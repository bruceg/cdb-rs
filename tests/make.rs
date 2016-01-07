extern crate cdb;
use std::fs;

macro_rules! noerr {
    ( $e:expr ) => {
        if let Err(x) = $e {
            panic!("{}", x);
        }
    }
}

#[test]
fn test_make() {
    let filename = "tests/make.cdb";

    let file = fs::File::create(filename).unwrap();
    let mut cdb = cdb::CDBMake::new(file).unwrap();
    noerr!(cdb.add(b"one", b"Hello"));
    noerr!(cdb.add(b"two", b"Goodbye"));
    noerr!(cdb.add(b"one", b", World!"));
    noerr!(cdb.add(b"this key will be split across two reads", b"Got it."));
    noerr!(cdb.finish());

    let file = fs::File::open(filename).unwrap();
    let mut cdb = cdb::CDB::init(file).unwrap();
    assert_eq!(cdb.find(b"two").next().unwrap().unwrap(), b"Goodbye");
    assert_eq!(cdb.find(b"this key will be split across two reads").next().unwrap().unwrap(), b"Got it.");
    let mut i = cdb.find(b"one");
    assert_eq!(i.next().unwrap().unwrap(), b"Hello");
    assert_eq!(i.next().unwrap().unwrap(), b", World!");

    noerr!(fs::remove_file(filename));
}
