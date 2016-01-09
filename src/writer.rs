use std::fs;
use std::io;
use std::io::prelude::*;
use std::cmp::max;
use std::path;
use std::string;

use hash::hash;
use uint32::*;

pub use std::io::Result;

#[derive(Clone,Copy,Debug)]
struct HashPos {
    hash: u32,
    pos: u32,
}

fn err_toobig<T>() -> Result<T> {
    Err(io::Error::new(io::ErrorKind::Other, "File too big"))
}

fn uint32_pack_hp(buf: &mut [u8], hp: &HashPos) {
    uint32_pack2(buf, hp.hash, hp.pos);
}

/// Base interface for making a CDB file.
pub struct CDBMake {
    entries: Vec<HashPos>,
    pos: u32,
    file: io::BufWriter<fs::File>,
}

impl CDBMake {

    /// Create a new CDB maker.
    pub fn new(file: fs::File) -> Result<CDBMake> {
        let mut w = io::BufWriter::new(file);
        let buf = [0; 2048];
        try!(w.seek(io::SeekFrom::Start(0)));
        try!(w.write(&buf));
        Ok(CDBMake{
            entries: Vec::new(),
            pos: 2048,
            file: w,
        })
    }

    fn pos_plus(&mut self, len: u32) -> Result<()> {
        if self.pos + len < len {
            err_toobig()
        }
        else {
            self.pos += len;
            Ok(())
        }
    }

    fn add_end(&mut self, keylen: u32, datalen: u32, hash: u32) -> Result<()> {
        self.entries.push(HashPos{ hash: hash, pos: self.pos });
        try!(self.pos_plus(8));
        try!(self.pos_plus(keylen));
        try!(self.pos_plus(datalen));
        Ok(())
    }

    fn add_begin(&mut self, keylen: u32, datalen: u32) -> Result<()> {
        let mut buf = [0; 8];
        uint32_pack2(&mut buf[0..8], keylen, datalen);
        try!(self.file.write(&buf));
        Ok(())
    }

    /// Add a record to the CDB file.
    pub fn add(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        if key.len() >= 0xffffffff || data.len() >= 0xffffffff {
            return Err(io::Error::new(io::ErrorKind::Other, "Key or data too big"));
        }
        try!(self.add_begin(key.len() as u32, data.len() as u32));
        try!(self.file.write(key));
        try!(self.file.write(data));
        self.add_end(key.len() as u32, data.len() as u32, hash(&key[..]))
    }

    /// Finish writing to the CDB file and flush its contents.
    pub fn finish(&mut self) -> Result<()> {
        let mut buf = [0; 8];

        let mut count = [0 as u32; 256];
        for e in self.entries.iter() {
            count[(e.hash & 255) as usize] += 1;
        }

        let mut memsize = count.iter().fold(1, |acc, c| max(acc, c * 2));
        memsize += self.entries.len() as u32;
        if memsize > (0xffffffff / 8) {
            return err_toobig();
        }

        let mut start = [0 as u32; 256];
        let mut u = 0;
        for i in 0..256 {
            u += count[i];
            start[i] = u;
        }

        let mut split = vec![HashPos{ hash: 0, pos: 0 }; memsize as usize];

        // The rev matches the original CDB logic, and outputs the entries in the same order.
        for e in self.entries.iter().rev() {
            let h = (e.hash & 255) as usize;
            start[h] -= 1;
            split[start[h] as usize] = *e;
        }

        let mut header = [0 as u8; 2048];
        for i in 0..256 {
            let len = count[i] * 2;
            let j = i * 8;
            uint32_pack2(&mut header[j..j+8], self.pos, len);

            let mut hp = start[i];
            for _ in 0..count[i] {
                let mut wh = (split[hp as usize].hash >> 8) % len;
                while split[wh as usize + self.entries.len()].pos > 0 {
                    wh += 1;
                    if wh == len {
                        wh = 0;
                    }
                }
                split[wh as usize + self.entries.len()] = split[hp as usize];
                hp += 1;
            }

            for u in 0..len {
                uint32_pack_hp(&mut buf, &split[u as usize + self.entries.len()]);
                try!(self.file.write(&buf));
                try!(self.pos_plus(8));
            }
        }

        try!(self.file.flush());
        try!(self.file.seek(io::SeekFrom::Start(0)));
        try!(self.file.write(&header));
        try!(self.file.flush());
        Ok(())
    }
}

/// A CDB file writer which handles atomic updating.
///
/// Using this type, a CDB file is safely written by first creating a
/// temporary file, building the CDB structure into that temporary file,
/// and finally renaming that temporary file over the final file name.
/// If the temporary file is not properly finished (ie due to an error),
/// the temporary file is deleted when this writer is dropped.
pub struct CDBWriter {
    dstname: String,
    tmpname: String,
    cdb: CDBMake,
}

impl CDBWriter {

    /// Safely create a new CDB file.
    ///
    /// The suffix for the temporary file defaults to `".tmp"`.
    pub fn create<P: AsRef<path::Path> + string::ToString>(filename: P) -> Result<CDBWriter> {
        CDBWriter::with_suffix(filename, ".tmp")
    }

    /// Safely create a new CDB file, using a specific suffix for the temporary file.
    pub fn with_suffix<P: AsRef<path::Path> + string::ToString>(filename: P, suffix: &str) -> Result<CDBWriter> {
        let mut tmpname = filename.to_string();
        tmpname.push_str(suffix);
        CDBWriter::with_filenames(filename, &tmpname)
    }

    /// Safely create a new CDB file, using two specific file names.
    ///
    /// Note that the temporary file name must be on the same filesystem
    /// as the destination, or else the final rename will fail.
    pub fn with_filenames<P: AsRef<path::Path> + string::ToString,
                          Q: AsRef<path::Path> + string::ToString>(filename: P, tmpname: Q) -> Result<CDBWriter> {
        let file = try!(fs::File::create(&tmpname));
        let cdb = try!(CDBMake::new(file));
        Ok(CDBWriter {
            dstname: filename.to_string(),
            tmpname: tmpname.to_string(),
            cdb: cdb,
        })
    }

    /// Add a record to the CDB file.
    pub fn add(&mut self, key: &[u8], data: &[u8]) -> Result<()> {
        self.cdb.add(key, data)
    }

    /// Set permissions on the temporary file.
    ///
    /// This must be done before the file is finished, as the temporary
    /// file will no longer exist at that point.
    pub fn set_permissions(&mut self, perm: fs::Permissions) -> Result<()> {
        // This should be a method on the file itself to use fchmod, but
        // Rust doesn't have that yet.
        fs::set_permissions(&self.tmpname, perm)
    }

    pub fn finish(&mut self) -> Result<()> {
        try!(self.cdb.finish());
        try!(fs::rename(&self.tmpname, &self.dstname));
        self.tmpname.clear();
        Ok(())
    }
}

impl Drop for CDBWriter {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        if self.tmpname.len() > 0 {
            fs::remove_file(&self.tmpname);
        }
    }
}
