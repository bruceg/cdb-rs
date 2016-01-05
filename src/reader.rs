use std::fs;
use std::io;
use std::io::Seek;
use std::io::Read;
use std::cmp::min;
pub use std::io::Result;

use hash::hash;
use uint32::*;

const KEYSIZE: usize = 32;

struct CDB {
    file: fs::File,
    header: [u8; 2048],
    kloop: u32,
    khash: u32,
    kpos: u32,
    hpos: u32,
    hslots: u32,
    dpos: u32,
    dlen: u32,
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
            kloop: 0,
            khash: 0,
            kpos: 0,
            hpos: 0,
            hslots: 0,
            dpos: 0,
            dlen: 0,
        })
    }

    fn read(&mut self, buf: &mut [u8], pos: u32) -> Result<usize> {
        try!(self.file.seek(io::SeekFrom::Start(pos as u64)));
        let mut len = buf.len();
        let mut read = 0;
        while len > 0 {
            let r = try!(self.file.read(&mut buf[read..]));
            len -= r;
            read += r;
            // FIXME: Handle r == 0 -> EPROTO
        }
        Ok(read)
    }

    fn find_start(&mut self) {
        self.kloop = 0;
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

    fn find_next(&mut self, key: &[u8]) -> Result<bool> {
        if self.kloop == 0 {
            let u = hash(key);
            let x = ((u as usize) << 8) & 2047;
            self.hslots = uint32_unpack(&self.header[x+4..x+8]);
            if self.hslots == 0 {
                return Ok(false);
            }
            self.hpos = uint32_unpack(&self.header[x..x+4]);
            self.khash = u;
            self.kpos = self.hpos + ((u >> 8) % self.hslots);
        }

        while self.kloop < self.hslots {
            let mut buf = [0 as u8; 8];
            let kpos = self.kpos;
            try!(self.read(&mut buf, kpos));
            let pos = uint32_unpack(&buf[4..8]);
            if pos == 0 {
                return Ok(false);
            }
            self.kloop += 1;
            self.kpos += 8;
            if self.kpos == self.hpos + (self.hslots << 3) {
                self.kpos = self.hpos;
            }
            let u = uint32_unpack(&buf[0..4]);
            if u == self.khash {
                try!(self.read(&mut buf, pos));
                let u = uint32_unpack(&buf[0..4]);
                if u as usize == key.len() {
                    if try!(self.match_key(key, pos + 8)) {
                        self.dlen = uint32_unpack(&buf[4..8]);
                        self.dpos = pos + 8 + key.len() as u32; // FIXME usize vs u32
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn find(&mut self, key: &[u8]) -> Result<bool> {
        self.find_start();
        return self.find_next(key);
    }
}

impl Drop for CDB {
    fn drop(&mut self) {
    }
}
