use std::{
    fs::File,
    io::{self, BufWriter, Seek, SeekFrom, Write},
    os::unix::ffi::OsStrExt,
    path::PathBuf,
};

use byteorder::{LittleEndian, WriteBytesExt};

use crate::{
    index::{Document, InMemoryIndex},
    tmp::TmpDir,
};

/// A structure to manage writing to an index file efficiently.
pub struct IndexFileWriter {
    /// Tracks the current write position in the file.
    offset: u64,
    /// Buffered writer to handle file output.
    writer: BufWriter<File>,
    /// Buffer to store contents entries before they are written.
    contents_buf: Vec<u8>,
}

impl IndexFileWriter {
    /// Constructs a new `IndexFileWriter`.
    ///
    /// Initializes the file with a header size and sets the initial offset.
    /// The header will store the final size of the main data section of the file.
    ///
    /// # Arguments
    /// * `f` - A buffered writer pre-configured to write to the target file
    ///
    /// # Errors
    /// Returns an error if writing the initial header fails.
    pub fn new(mut f: BufWriter<File>) -> io::Result<IndexFileWriter> {
        const HEADER_SIZE: u64 = 8;
        f.write_u64::<LittleEndian>(0)?; // content start
        Ok(IndexFileWriter {
            offset: HEADER_SIZE,
            writer: f,
            contents_buf: vec![],
        })
    }

    /// Writes a buffer to the file and updates the offset.
    ///
    /// # Arguments
    /// * `buf` - The data to write to the file
    ///
    /// # Errors
    /// Returns an error if the write operation fails.
    pub fn write_main(&mut self, buf: &[u8]) -> io::Result<()> {
        self.writer.write_all(buf)?;
        self.offset += buf.len() as u64;
        Ok(())
    }

    /// Appends a content entry to the internal buffer.
    ///
    /// # Arguments
    /// * `term` - The term associated with the entry
    /// * `df` - Document frequency for the term
    /// * `offset` - Offset where the term data starts in the file
    /// * `nbytes` - Number of bytes of the term data
    pub fn write_contents_entry(&mut self, term: String, df: u32, offset: u64, nbytes: u64) {
        self.contents_buf.write_u64::<LittleEndian>(offset).unwrap();
        self.contents_buf.write_u64::<LittleEndian>(nbytes).unwrap();
        self.contents_buf.write_u32::<LittleEndian>(df).unwrap();
        let bytes = term.bytes();
        self.contents_buf
            .write_u32::<LittleEndian>(bytes.len() as u32)
            .unwrap();
        self.contents_buf.extend(bytes);
    }

    /// Writes a `Document` object to the file associated with the `IndexFileWriter`.
    ///
    /// This function serializes a `Document` object and writes it to the underlying `BufWriter<File>`.
    /// The serialization format is as follows:
    /// - Document ID (u32)
    /// - Path length (u64) followed by Path bytes (variable length)
    ///
    /// The offsets are updated accordingly after each write to ensure the correct position
    /// for subsequent writes.
    pub fn write_document(&mut self, doc: &Document) -> io::Result<()> {
        self.writer.write_u32::<LittleEndian>(doc.id)?;
        self.writer
            .write_u64::<LittleEndian>(doc.path.as_os_str().len() as u64)?;
        self.writer.write_all(doc.path.as_os_str().as_bytes())?;
        self.offset += 4 + 8 + doc.path.as_os_str().len() as u64;
        Ok(())
    }

    /// Completes the writing process to the index file and finalizes the file structure.
    ///
    /// This method first writes the accumulated contents entries from the buffer to the file.
    /// It then updates the file header with the size of the document section and the starting
    /// position of the contents section, which are crucial for readers to correctly interpret the file data.
    /// The method ensures all data is flushed to disk and the file is left in a consistent state.
    pub fn finish(mut self) -> io::Result<()> {
        let contents_start = self.offset;
        self.writer.write_all(&self.contents_buf)?;
        println!(
            "{} bytes main, {} bytes total",
            contents_start,
            contents_start + self.contents_buf.len() as u64
        );
        self.writer.seek(SeekFrom::Start(0))?;
        self.writer.write_u64::<LittleEndian>(contents_start)?;
        Ok(())
    }
}

/// Writes an in-memory index to a temporary file using a structured binary format.
///
/// This function serializes the contents of an `InMemoryIndex` and writes them into a temporary file.
/// It organizes the data into two main sections: a document section and an index section. Each section
/// is preceded by its own size metadata. The file is structured to allow efficient data retrieval based
/// on the written index and can be used in applications requiring fast lookups.
pub fn write_index_to_tmp_file(index: InMemoryIndex, tmp_dir: &mut TmpDir) -> io::Result<PathBuf> {
    let (filename, f) = tmp_dir.create()?;
    let mut writer = IndexFileWriter::new(f)?;

    let mut index_as_vec: Vec<_> = index.terms.into_iter().collect();
    index_as_vec.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (term, hits) in index_as_vec {
        let df = hits.len() as u32;
        let start = writer.offset;
        for buffer in hits {
            writer.write_main(&buffer)?;
        }
        let stop = writer.offset;
        writer.write_contents_entry(term, df, start, stop - start);
    }

    // if term == "" && df == 0 { type = document }
    for (_, doc) in index.docs {
        let start = writer.offset;
        writer.write_document(&doc)?;
        let stop = writer.offset;
        writer.write_contents_entry("".to_string(), 0, start, stop - start)
    }

    writer.finish()?;
    println!("wrote file {:?}", filename);
    Ok(filename)
}
