//! This crate provides support for reading and writing
//! [CDB](https://cr.yp.to/cdb.html) files. A CDB is a "constant
//! database" that acts as an on-disk associative array mapping keys to
//! values, allowing multiple values for each key. It provides for fast
//! lookups and low overheads. A constant database has no provision for
//! updating, only rewriting from scratch.
//!
//! # Examples
//!
//! Reading a set of records:
//!
//! ```
//! let cdb = cdb::CDB::open("tests/test1.cdb").unwrap();
//!
//! for result in cdb.find(b"one") {
//!     println!("{:?}", result.unwrap());
//! }
//! ```
//!
//! Creating a database with safe atomic updating:
//!
//! ```no_run
//! fn main() -> std::io::Result<()> {
//!     let mut cdb = cdb::CDBWriter::create("temporary.cdb")?;
//!     cdb.add(b"one", b"Hello, ")?;
//!     cdb.add(b"one", b"world!\n")?;
//!     cdb.add(b"two", &[1, 2, 3, 4])?;
//!     cdb.finish()?;
//!     Ok(())
//! }
//! ```
//!
//! # References
//!
//!  * [D. J. Bernstein's original software](https://cr.yp.to/cdb.html)
//!  * [Constant Database (cdb) Internals](https://www.unixuser.org/~euske/doc/cdbinternals/index.html)
//!  * [Wikipedia](https://en.wikipedia.org/wiki/Cdb_(software))

extern crate filebuffer;

mod uint32;
mod hash;
mod reader;
mod writer;

pub use reader::{CDB, CDBIter, CDBKeyValueIter, CDBValueIter, Result};
pub use writer::{CDBMake, CDBWriter};
