use std::{
    fs::{self, File},
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use byteorder::*;

use byteorder::LittleEndian;

/// File Memory Structure Diagram
///
/// +----------------------------------+-----------------------------------+
/// |                               Header                                 |
/// +----------------------------------+-----------------------------------+
/// | Index Size (8 bytes)             | Document Size (8 bytes)           |
/// +----------------------------------+-----------------------------------+
/// |                                                                      |
/// |                      Document Section                                |
/// |----------------------------------------------------------------------|
/// | Document 1 Data ...                                                  |
/// |   - Document ID (4 bytes)                                            |
/// |   - Path Length (8 bytes)                                            |
/// |   - Path Data (variable length)                                      |
/// | Document 2 Data ...                                                  |
/// |   ...                                                                |
/// | Document N Data ...                                                  |
/// |   ...                                                                |
/// +----------------------------------------------------------------------+
/// |                                                                      |
/// |                        Index Section                                 |
/// |----------------------------------------------------------------------|
/// | Index Entry 1 Data ...                                               |
/// |   - Entry Data (variable length, based on term hits)                 |
/// | Index Entry 2 Data ...                                               |
/// |   ...                                                                |
/// | Index Entry M Data ...                                               |
/// |   ...                                                                |
/// +----------------------------------------------------------------------+
/// |                                                                      |
/// |                      Contents Table Section                          |
/// |----------------------------------------------------------------------|
/// | Contents Entry 1 ...                                                 |
/// |   - Offset (8 bytes)                                                 |
/// |   - Length (8 bytes)                                                 |
/// |   - Document Frequency (DF) (4 bytes)                                |
/// |   - Term Length (4 bytes)                                            |
/// |   - Term Data (variable length)                                      |
/// | Contents Entry 2 ...                                                 |
/// |   ...                                                                |
/// | Contents Entry M ...                                                 |
/// |   ...                                                                |
/// +----------------------------------------------------------------------+
///
/// Description:
/// - Header:
///   - Index Size: Stores the size of the index section, allowing quick navigation to the index data.
///   - Document Size: Stores the total size of the document section, helping in locating and managing document data within the file.
/// - Document Section:
///   - Contains serialized document data including unique identifiers and paths for each document.
/// - Index Section:
///   - Contains the main data entries for the index, stored consecutively. Each entry's length may vary depending on the actual data.
/// - Contents Table Section:
///   - Provides metadata for quick access to index entries, including offsets and lengths, document frequency, and terms associated with each entry.

pub struct IndexFileReader {
    main: BufReader<File>,
    contents: BufReader<File>,
    next: Option<Entry>,
}

pub struct Entry {
    pub term: String,
    pub df: u32,
    pub offset: u64,
    pub nbytes: u64,
}

impl IndexFileReader {
    pub fn open_and_delete<P: AsRef<Path>>(filename: P) -> io::Result<IndexFileReader> {
        let mut main_raw = File::open(filename)?;

        // header
        let document_size = main_raw.read_u64::<LittleEndian>()?;
        let contents_offset = main_raw.read_u64::<LittleEndian>()?;

        let mut contents_raw = File::open(filename)?;
        contents_raw.seek(SeekFrom::Start(contents_offset))?;
        let main = BufReader::new(main_raw);
        let mut contents = BufReader::new(contents_raw);

        let first = IndexFileReader::read_entry(&mut contents)?;
        fs::remove_file(filename)?;

        Ok(IndexFileReader {
            main,
            contents,
            next: first,
        })
    }

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
        let mut bytes = Vec::with_capacity(term_len);
        bytes.resize(term_len, 0);
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
}
