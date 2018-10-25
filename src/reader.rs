use filebuffer::FileBuffer;
use std::io;
use std::cmp::min;
use std::path;

use hash::hash;
use uint32;

pub use std::io::Result;

const KEYSIZE: usize = 32;

/// CDB file reader
///
/// # Example
///
/// ```
/// let cdb = cdb::CDB::open("tests/test1.cdb").unwrap();
///
/// for result in cdb.find(b"one") {
///     println!("{:?}", result.unwrap());
/// }
/// ```
pub struct CDB {
    file: FileBuffer,
    size: usize,
}

fn err_badfile<T>() -> Result<T> {
    Err(io::Error::new(io::ErrorKind::Other, "Invalid file format"))
}

impl CDB {

    /// Opens the named file and returns the CDB reader.
    ///
    /// # Examples
    ///
    /// ```
    /// let cdb = cdb::CDB::open("tests/test1.cdb").unwrap();
    /// ```
    pub fn open<P: AsRef<path::Path>>(filename: P) -> Result<CDB> {
        let file = FileBuffer::open(&filename)?;
        if file.len() < 2048 + 8 + 8 || file.len() > 0xffffffff {
            return err_badfile();
        }
        let size = file.len();
        Ok(CDB {
            file,
            size,
        })
    }

    fn read(&self, buf: &mut [u8], pos: u32) -> Result<usize> {
        let len = buf.len();
        let pos = pos as usize;
        if pos + len > self.size {
            return err_badfile();
        }
        buf.copy_from_slice(&self.file[pos .. pos + len]);
        Ok(len)
    }

    fn hash_table(&self, khash: u32) -> (u32, u32, u32) {
        let x = ((khash as usize) & 0xff) << 3;
        let (hpos, hslots) = uint32::unpack2(&self.file[x..x+8]);
        let kpos = if hslots > 0 { hpos + (((khash >> 8) % hslots) << 3) } else { 0 };
        (hpos, hslots, kpos)
    }

    fn match_key(&self, key: &[u8], pos: u32) -> Result<bool> {
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

    /// Find all records with the named key. The returned iterator
    /// produces each value associated with the key.
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
    pub fn find(&self, key: &[u8]) -> CDBValueIter {
        CDBValueIter::find(self, key)
    }
}

pub type CDBIter<'a> = CDBValueIter<'a>;

/// Iterator over a set of records in the CDB with the same key.
pub struct CDBValueIter<'a> {
    cdb: &'a CDB,
    key: Vec<u8>,
    khash: u32,
    kloop: u32,
    kpos: u32,
    hpos: u32,
    hslots: u32,
    dpos: u32,
    dlen: u32,
}

impl<'a> CDBValueIter<'a> {
    fn find(cdb: &'a CDB, key: &[u8]) -> CDBValueIter<'a> {
        let khash = hash(key);
        let (hpos, hslots, kpos) = cdb.hash_table(khash);

        CDBValueIter {
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

    fn read_vec(&self) -> Result<Vec<u8>> {
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

impl<'a> Iterator for CDBValueIter<'a> {
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
