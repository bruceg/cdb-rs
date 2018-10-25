extern crate cdb;
#[macro_use]
extern crate criterion;

use cdb::CDB;
use criterion::Criterion;

fn test_cdb() -> CDB {
    CDB::open("tests/test1.cdb").expect("Could not open tests/test1.cdb")
}

fn reader_benchmark(c: &mut Criterion) {
    c.bench_function("CDB::open", |b| b.iter(|| { test_cdb(); }));
    c.bench_function("CDB::find", |b| {
        let cdb = test_cdb();
        b.iter(|| cdb.find(b"two"))
    });
    c.bench_function("CDB::find long", |b| {
        let cdb = test_cdb();
        b.iter(|| cdb.find(b"this key will be split across two reads"))
    });
    c.bench_function("CDB::find result", |b| {
        let cdb = test_cdb();
        b.iter(|| cdb.find(b"two").next().unwrap())
    });
    c.bench_function("CDB::find result loop", |b| {
        let cdb = test_cdb();
        b.iter(|| for result in cdb.find(b"one") { result.unwrap(); })
    });
    c.bench_function("CDB::open + find result loop", |b| b.iter(|| {
        let cdb = test_cdb();
        for result in cdb.find(b"one") {
            result.unwrap();
        }
    }));
    c.bench_function("CDB::iter result loop", |b| {
        let cdb = test_cdb();
        b.iter(|| for result in cdb.iter() { result.unwrap(); })
    });
    c.bench_function("CDB::open + iter result loop", |b| b.iter(|| {
        let cdb = test_cdb();
        for result in cdb.iter() {
            result.unwrap();
        }
    }));
}

criterion_group!(benches, reader_benchmark);
criterion_main!(benches);
