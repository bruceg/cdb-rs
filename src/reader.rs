use std::fs;
use std::io;
use std::io::prelude::*;
use std::cmp::min;

use hash::hash;
use uint32::*;

pub use std::io::Result;

const KEYSIZE: usize = 32;

pub struct CDB {
    file: fs::File,
    header: [u8; 2048],
}

impl CDB {
    pub fn init(f: fs::File) -> Result<CDB> {
        let mut buf = [0; 2048];
        let mut f = f;
        try!(f.seek(io::SeekFrom::Start(0)));
        try!(f.read(&mut buf));
        Ok(CDB {
            file: f,
            header: buf,
        })
    }

    fn read(&mut self, buf: &mut [u8], pos: u32) -> Result<usize> {
        try!(self.file.seek(io::SeekFrom::Start(pos as u64)));
        let mut len = buf.len();
        let mut read = 0;
        while len > 0 {
            let r = try!(self.file.read(&mut buf[read..]));
            if r == 0 {
                // Should use ErrorKind::UnexpectedEOF, but it's still unstable
                return Err(io::Error::new(io::ErrorKind::Other, "Invalid file format"));
            }
            len -= r;
            read += r;
        }
        Ok(read)
    }

    fn hash_table(&self, khash: u32) -> (u32, u32, u32) {
        let x = ((khash as usize) & 0xff) << 3;
        let hpos = uint32_unpack(&self.header[x..x+4]);
        let hslots = uint32_unpack(&self.header[x+4..x+8]);
        let kpos = if hslots > 0 { hpos + (((khash >> 8) % hslots) << 3) } else { 0 };
        (hpos, hslots, kpos)
    }

    fn match_key(&mut self, key: &[u8], pos: u32) -> Result<bool> {
        let mut buf = [0 as u8; KEYSIZE];
        let mut len = key.len();
        let mut pos = pos;
        let mut keypos = 0;

        while len > 0 {
            let n = min(len, buf.len());
            try!(self.read(&mut buf[..n], pos));
            if buf[..n] != key[keypos..keypos+n] {
                return Ok(false);
            }
            pos += n as u32;
            keypos += n;
            len -= n;
        }
        Ok(true)
    }

    pub fn find(&mut self, key: &[u8]) -> CDBIter {
        CDBIter::find(self, key)
    }
}

pub struct CDBIter<'a> {
    cdb: &'a mut CDB,
    key: Vec<u8>,
    khash: u32,
    kloop: u32,
    kpos: u32,
    hpos: u32,
    hslots: u32,
    dpos: u32,
    dlen: u32,
}

impl<'a> CDBIter<'a> {
    fn find(cdb: &'a mut CDB, key: &[u8]) -> CDBIter<'a> {
        let khash = hash(key);
        let (hpos, hslots, kpos) = cdb.hash_table(khash);

        CDBIter {
            cdb: cdb,
            key: key.into_iter().map(|x| *x).collect(),
            khash: khash,
            kloop: 0,
            kpos: kpos,
            hpos: hpos,
            hslots: hslots,
            dpos: 0,
            dlen: 0,
        }
    }

    fn read_vec(&mut self) -> Result<Vec<u8>> {
        let mut result = vec![0; self.dlen as usize];
        try!(self.cdb.read(&mut result[..], self.dpos));
        Ok(result)
    }
}

macro_rules! iter_try {
    ( $e:expr ) => {
        match $e {
            Err(x) => { return Some(Err(x)); },
            Ok(y) => y
        }
    }
}

impl<'a> Iterator for CDBIter<'a> {
    type Item = Result<Vec<u8>>;
    fn next(&mut self) -> Option<Result<Vec<u8>>> {
        while self.kloop < self.hslots {
            let mut buf = [0 as u8; 8];
            let kpos = self.kpos;
            iter_try!(self.cdb.read(&mut buf, kpos));
            let pos = uint32_unpack(&buf[4..8]);
            let khash = uint32_unpack(&buf[0..4]);
            if pos == 0 {
                return None;
            }
            self.kloop += 1;
            self.kpos += 8;
            if self.kpos == self.hpos + (self.hslots << 3) {
                self.kpos = self.hpos;
            }
            if khash == self.khash {
                iter_try!(self.cdb.read(&mut buf, pos));
                let klen = uint32_unpack(&buf[0..4]);
                if klen as usize == self.key.len() {
                    if iter_try!(self.cdb.match_key(&self.key[..], pos + 8)) {
                        self.dlen = uint32_unpack(&buf[4..8]);
                        self.dpos = pos + 8 + self.key.len() as u32;
                        return Some(self.read_vec());
                    }
                }
            }
        }
        None
    }
}

#[test]
fn test_one() {
    //let test1[u8] = include_bytes!("tests/test1.cdb");
    let f = fs::File::open("tests/test1.cdb").unwrap();
    let mut cdb = CDB::init(f).unwrap();
    let mut i = cdb.find(b"one");
    assert_eq!(i.next().unwrap().unwrap(), b"Hello");
    assert_eq!(i.next().unwrap().unwrap(), b", World!");
}


#[test]
fn test_two() {
    //let test1[u8] = include_bytes!("tests/test1.cdb");
    let f = fs::File::open("tests/test1.cdb").unwrap();
    let mut cdb = CDB::init(f).unwrap();
    assert_eq!(cdb.find(b"two").next().unwrap().unwrap(), b"Goodbye");
    assert_eq!(cdb.find(b"this key will be split across two reads").next().unwrap().unwrap(), b"Got it.");
}
