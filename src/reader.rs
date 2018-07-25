extern crate libc;
extern crate mmap;

use std::fs;
use std::io;
use std::io::prelude::*;
use std::cmp::min;
use std::path;
use std::ptr;
use std::slice;

use hash::hash;
use uint32;

pub use std::io::Result;

const KEYSIZE: usize = 32;

/// CDB file reader
pub struct CDB {
    file: io::BufReader<fs::File>,
    size: usize,
    pos: u32,
    mmap: Option<mmap::MemoryMap>,
    header: [u8; 2048],
}

fn err_badfile<T>() -> Result<T> {
    Err(io::Error::new(io::ErrorKind::Other, "Invalid file format"))
}

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(unix)]
fn get_fd(file: &fs::File) -> libc::c_int {
    file.as_raw_fd()
}

#[cfg(windows)]
fn get_fd(file: &fs::File) -> libc::HANDLE {
    file.as_raw_handle()
}

fn mmap_file(file: &fs::File, len: usize) -> Result<mmap::MemoryMap> {
    let fd = get_fd(file);
    match mmap::MemoryMap::new(len, &[
        mmap::MapOption::MapReadable,
        mmap::MapOption::MapFd(fd),
        ]) {
        Err(_) => Err(io::Error::new(io::ErrorKind::Other, "mmap failed")),
        Ok(x) => Ok(x),
    }
}

impl CDB {

    /// Constructs a new CDB by opening a file.
    ///
    /// # Examples
    ///
    /// ```
    /// let cdb = cdb::CDB::open("tests/test1.cdb").unwrap();
    /// ```
    pub fn open<P: AsRef<path::Path>>(filename: P) -> Result<CDB> {
        let file = try!(fs::File::open(&filename));
        let mut buf = [0; 2048];
        let meta = try!(file.metadata());
        let mut file = io::BufReader::new(file);
        if meta.len() < 2048 + 8 + 8 || meta.len() > 0xffffffff {
            return err_badfile();
        }
        let map = if let Ok(m) = mmap_file(&file.get_ref(), meta.len() as usize) {
            Some(m)
        }
        else {
            try!(file.seek(io::SeekFrom::Start(0)));
            try!(file.read(&mut buf));
            None
        };
        Ok(CDB {
            file,
            header: buf,
            pos: 2048,
            size: meta.len() as usize,
            mmap: map,
        })
    }

    fn read(&mut self, buf: &mut [u8], pos: u32) -> Result<usize> {
        if pos as usize + buf.len() > self.size {
            return err_badfile();
        }
        if let Some(ref map) = self.mmap {
            unsafe {
                ptr::copy_nonoverlapping(map.data().offset(pos as isize), buf.as_mut_ptr(), buf.len());
            }
            Ok(buf.len())
        }
        else {
            if pos != self.pos {
                try!(self.file.seek(io::SeekFrom::Start(pos as u64)));
            }
            let mut len = buf.len();
            let mut read = 0;
            while len > 0 {
                let r = try!(self.file.read(&mut buf[read..]));
                len -= r;
                read += r;
            }
            Ok(read)
        }
    }

    fn hash_table(&self, khash: u32) -> (u32, u32, u32) {
        let x = ((khash as usize) & 0xff) << 3;
        let (hpos, hslots) = if let Some(ref map) = self.mmap {
            let s = unsafe { slice::from_raw_parts(map.data(), 2048) };
            uint32::unpack2(&s[x..x+8])
        }
        else {
            uint32::unpack2(&self.header[x..x+8])
        };
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

    /// Find all records with the named key.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut cdb = cdb::CDB::open("tests/test1.cdb").unwrap();
    ///
    /// for result in cdb.find(b"one") {
    ///     println!("{:?}", result.unwrap());
    /// }
    /// ```
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
            let (khash, pos) = uint32::unpack2(&buf);
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
                let (klen, dlen) = uint32::unpack2(&buf);
                if klen as usize == self.key.len() {
                    if iter_try!(self.cdb.match_key(&self.key[..], pos + 8)) {
                        self.dlen = dlen;
                        self.dpos = pos + 8 + self.key.len() as u32;
                        return Some(self.read_vec());
                    }
                }
            }
        }
        None
    }
}
