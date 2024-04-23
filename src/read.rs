use std::{
    fs::{self, File},
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use byteorder::*;

use byteorder::LittleEndian;

use crate::write::IndexFileWriter;

/// A `IndexFileReader` does a single linear pass over an index file from
/// beginning to end. Needless to say, this is not how an index is normally
/// used! This is used only when merging multiple index files.
///
/// The only way to advance through the file is to use the `.move_entry_to()`
/// method.
pub struct IndexFileReader {
    /// Reader that reads the actual terms and docs data.
    ///
    /// We have two readers. The terms and docs data is most of the file.
    /// There's also a table of entries, stored separately at the end.
    /// We have to read them in tandem, so we open the file twice.
    pub terms_docs: BufReader<File>,

    /// Reader that reads the table of entries. (Since this table is stored at
    /// the end of the file, we have to begin by `seek`ing to it; see the code
    /// in `IndexFileReader::open_and_delete`.)
    entries: BufReader<File>,

    /// The next entry in the table of contents, if any; or `None` if we've
    /// reached the end of the table. `IndexFileReader` always reads ahead one
    /// entry in the contents and stores it here.
    next: Option<Entry>,
}

/// An entry in the table of entries of an index file.
///
/// Each entry in the table of entries is small. It consists of a string, the
/// `term`; summary information about that term, as used in the corpus (`df`);
/// and a pointer to bulkier data that tells more (`offset` and `nbytes`).
pub struct Entry {
    /// The term is a word that appears in one or more documents in the corpus.
    /// The index file contains information about the documents that use this
    /// word.
    pub term: String,

    /// Total number of documents in the corpus that contain this term.
    pub df: u32,

    /// Offset of the index data for this term from the beginning of the file, in bytes.
    pub offset: u64,

    /// Length of the index data for this term, in bytes.
    pub nbytes: u64,
}

impl IndexFileReader {
    /// Open an index file to read it from beginning to end.
    /// Optionally deletes the index file after opening if `delete` is true.
    /// Returns an `IndexFileReader` or an I/O error.
    ///
    /// - `filename`: path to the index file.
    /// - `delete`: whether to delete the file after opening.
    pub fn open_and_delete<P: AsRef<Path>>(
        filename: P,
        delete: bool,
    ) -> io::Result<IndexFileReader> {
        let filename = filename.as_ref();
        let mut terms_docs_raw = File::open(filename)?;

        // header
        let entries_offset = terms_docs_raw.read_u64::<LittleEndian>()?;
        println!(
            "opened {}, table of entries starts at {}",
            filename.display(),
            entries_offset
        );

        let mut entries_raw = File::open(filename)?;
        entries_raw.seek(SeekFrom::Start(entries_offset))?;
        let terms_docs = BufReader::new(terms_docs_raw);
        let mut entries = BufReader::new(entries_raw);

        let first = IndexFileReader::read_entry(&mut entries)?;

        if delete {
            fs::remove_file(filename)?;
        }

        Ok(IndexFileReader {
            terms_docs,
            entries,
            next: first,
        })
    }

    /// Read the next entry from the table of contents.
    ///
    /// Returns `Ok(None)` if we have reached the end of the file.
    fn read_entry(f: &mut BufReader<File>) -> io::Result<Option<Entry>> {
        let offset = match f.read_u64::<LittleEndian>() {
            Ok(value) => value,
            Err(err) => {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                } else {
                    return Err(err);
                }
            }
        };

        let nbytes = f.read_u64::<LittleEndian>()?;
        let df = f.read_u32::<LittleEndian>()?;
        let term_len = f.read_u32::<LittleEndian>()? as usize;
        let mut bytes = vec![0; term_len];
        f.read_exact(&mut bytes)?;
        let term = match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "unicode fail")),
        };

        Ok(Some(Entry {
            term,
            df,
            offset,
            nbytes,
        }))
    }

    /// Borrow a reference to the next entry in the table of contents.
    /// (Since we always read ahead one entry, this method can't fail.)
    ///
    /// Returns `None` if we've reached the end of the file.
    pub fn peek(&self) -> Option<&Entry> {
        self.next.as_ref()
    }

    /// Advances the reader to the next entry and returns the current entry.
    pub fn iter_next_entry(&mut self) -> Option<Entry> {
        let res = self.next.take();
        if let Ok(n) = Self::read_entry(&mut self.entries) {
            self.next = n
        }
        res
    }

    /// True if the next entry is for the given term.
    pub fn is_at(&self, term: &str) -> bool {
        match self.next {
            Some(ref e) => e.term == term,
            None => false,
        }
    }

    /// Copy the current entry to the specified output stream, then read the
    /// header for the next entry.
    pub fn move_entry_to(&mut self, out: &mut IndexFileWriter) -> io::Result<()> {
        {
            let e = self.next.as_ref();
            if e.is_none() {
                return Err(io::Error::new(io::ErrorKind::Other, "no entry to move"));
            }
            let e = e.unwrap();
            if e.nbytes > usize::max_value() as u64 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "computer not big enough to hold index entry",
                ));
            }
            let mut buf = vec![0; e.nbytes as usize];
            self.terms_docs.read_exact(&mut buf)?;
            out.write_main(&buf)?;
        }

        self.next = Self::read_entry(&mut self.entries)?;
        Ok(())
    }
}
