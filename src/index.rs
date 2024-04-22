//! In-memory indexes.
//!
//! The first step in building the index is to index documents in memory.
//! `InMemoryIndex` can be used to do that, up to the size of the machine's
//! memory.

use std::{
    collections::HashMap,
    ffi::OsString,
    io::{self, Cursor, Read, Seek},
    os::unix::ffi::OsStringExt,
    path::{Path, PathBuf},
    vec,
};

use byteorder::*;

use crate::read::IndexFileReader;

/// Break a string into words.
fn tokenize(text: &str) -> Vec<&str> {
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect()
}

/// An in-memory index.
///
/// Of course, a real index for a large corpus of documents won't fit in
/// memory. But apart from memory constraints, this is everything you need to
/// answer simple search queries. And you can use the `read`, `write`, and
/// `merge` modules to save an in-memory index to disk and merge it with other
/// indices, producing a large index.
pub struct InMemoryIndex {
    /// The total number of words in the indexed documents.
    pub word_count: usize,

    /// For every term that appears in the index, the list of all search hits
    /// for that term (i.e. which documents contain that term, and where).
    ///
    /// It's possible for an index to be "sorted by document id", which means
    /// that for every `Vec<Hit>` in this map, the `Hit` elements all have
    /// distinct document ids (the first u32) and the `Hit`s are arranged by
    /// document id in increasing order. This is handy for some algorithms you
    /// might want to run on the index, so we preserve this property wherever
    /// possible.
    pub map: HashMap<String, Vec<Hit>>,

    pub docs: HashMap<u32, Document>,
}

pub struct Document {
    pub id: u32,
    pub path: PathBuf,
}

/// A `Hit` indicates that a particular document contains some term, how many
/// times it appears, and at what offsets (that is, the word count, from the
/// beginning of the document, of each place where the term appears).
///
/// The buffer contains all the hit data in binary form, little-endian. The
/// first u32 of the data is the document id. The remaining [u32] are offsets.
pub type Hit = Vec<u8>;

impl InMemoryIndex {
    /// Create a new, empty index.
    pub fn new() -> InMemoryIndex {
        InMemoryIndex {
            word_count: 0,
            map: HashMap::new(),
            docs: HashMap::new(),
        }
    }

    pub fn from_index_file<P: AsRef<Path>>(filename: P) -> io::Result<InMemoryIndex> {
        let mut index = InMemoryIndex::new();
        let mut reader = IndexFileReader::open_and_delete(filename, false)?;

        while let Some(entry) = reader.iter_next_entry() {
            if entry.term.is_empty() && entry.df == 0 {
                // documents
                reader.main.seek(io::SeekFrom::Start(entry.offset))?;
                let doc_id = reader.main.read_u32::<LittleEndian>()?;
                let path_len = reader.main.read_u64::<LittleEndian>()?;
                let mut path = vec![0u8; path_len as usize];
                reader.main.read_exact(&mut path)?;
                index.docs.insert(
                    doc_id,
                    Document {
                        id: doc_id,
                        path: vec_to_pathbuf(path),
                    },
                );
            } else {
                // entrys
                let mut hits = vec![];
                reader.main.seek(io::SeekFrom::Start(entry.offset))?;
                let mut data = vec![0u8; entry.nbytes as usize];
                reader.main.read_exact(&mut data)?;
                let mut cursor = Cursor::new(data);

                let mut i = entry.df;
                let mut has_hit = false;
                let mut quit = false;

                while i > 0 && !quit {
                    let mut hit = vec![0u8; 4 + 4];
                    loop {
                        if let Ok(item) = cursor.read_u32::<LittleEndian>() {
                            if item == 0 && has_hit {
                                // the start of next hit
                                hits.push(hit);
                                i -= 1;
                                vec![0u8; 4 + 4];
                                break;
                            }
                            has_hit = true;
                            hit.write_u32::<LittleEndian>(item).unwrap();
                        } else {
                            quit = true;
                        }
                    }
                }
                index.map.insert(entry.term, hits);
            }
        }
        Ok(index)
    }

    /// Index a single document.
    ///
    /// The resulting index contains exactly one `Hit` per term.
    pub fn from_single_document(document_id: u32, path: PathBuf, text: String) -> InMemoryIndex {
        let mut index = InMemoryIndex::new();

        let text_lowercase = text.to_lowercase();
        let tokens = tokenize(&text_lowercase);
        for (i, token) in tokens.iter().enumerate() {
            let hits = index.map.entry(token.to_string()).or_insert_with(|| {
                let mut hits = Vec::with_capacity(4 + 4 + 4);
                const HITS_SEPERATOR: u32 = 0;
                hits.write_u32::<LittleEndian>(HITS_SEPERATOR).unwrap();
                hits.write_u32::<LittleEndian>(document_id).unwrap();
                vec![hits]
            });
            // start from 1, if read 0, means reach a Hits end.
            hits[0].write_u32::<LittleEndian>((i + 1) as u32).unwrap();
            index.word_count += 1;
        }

        println!(
            "indexed document {}:{:?}, {} bytes, {} words",
            document_id,
            &path,
            &text.len(),
            index.word_count
        );

        let _ = index.docs.insert(
            document_id,
            Document {
                id: document_id,
                path,
            },
        );

        index
    }

    /// Add all search hits from `other` to this index.
    ///
    /// If both `*self` and `other` are sorted by document id, and all document
    /// ids in `other` are greater than every document id in `*self`, then
    /// `*self` remains sorted by document id after merging.
    pub fn merge(&mut self, other: InMemoryIndex) {
        for (term, hits) in other.map {
            self.map.entry(term).or_default().extend(hits)
        }
        self.word_count += other.word_count;
        self.docs.extend(other.docs);
    }

    /// True if this index contains no data.
    pub fn is_empty(&self) -> bool {
        self.word_count == 0
    }

    /// True if this index is large enough that we should dump it to disk rather
    /// than keep adding more data to it.
    pub fn is_large(&self) -> bool {
        // This depends on how much memory your computer has, of course.
        const REASONABLE_SIZE: usize = 100_000_000;
        self.word_count > REASONABLE_SIZE
    }
}

impl Default for InMemoryIndex {
    fn default() -> Self {
        Self::new()
    }
}

fn vec_to_pathbuf(bytes: Vec<u8>) -> PathBuf {
    let os_string = OsString::from_vec(bytes);
    PathBuf::from(os_string)
}
