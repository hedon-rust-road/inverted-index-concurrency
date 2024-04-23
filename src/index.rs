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

#[derive(Debug, Default)]
pub struct TokenPos {
    pub start_pos: u32,
    pub end_pos: u32,
}

/// An in-memory index.
///
/// Of course, a real index for a large corpus of documents won't fit in
/// memory. But apart from memory constraints, this is everything you need to
/// answer simple search queries. And you can use the `read`, `write`, and
/// `merge` modules to save an in-memory index to disk and merge it with other
/// indices, producing a large index.
#[derive(Debug)]
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

#[derive(Debug)]
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
    const HITS_SEPERATOR: i32 = -1;

    /// Create a new, empty index.
    pub fn new() -> InMemoryIndex {
        InMemoryIndex {
            word_count: 0,
            map: HashMap::new(),
            docs: HashMap::new(),
        }
    }

    /// Index a single document.
    ///
    /// The resulting index contains exactly one `Hit` per term.
    pub fn from_single_document(document_id: u32, path: PathBuf, text: String) -> InMemoryIndex {
        let mut index = InMemoryIndex::new();

        let text_lowercase = text.to_lowercase();
        let tokens = tokenize(&text_lowercase);
        for (token, start_pos, end_pos) in tokens.iter() {
            let hits = index.map.entry(token.to_string()).or_insert_with(|| {
                let mut hits = Vec::with_capacity(4 + 4 + 4);
                hits.write_i32::<LittleEndian>(Self::HITS_SEPERATOR)
                    .unwrap();
                hits.write_u32::<LittleEndian>(document_id).unwrap();
                vec![hits]
            });

            hits[0]
                .write_u32::<LittleEndian>(*start_pos as u32)
                .unwrap();
            hits[0].write_u32::<LittleEndian>(*end_pos as u32).unwrap();
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

    // Load an InMemoryIndex from an index file.
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
                    let mut hit = Vec::with_capacity(4 + 4 + 4); // cannot use vec![0;12]
                    loop {
                        if let Ok(item) = cursor.read_i32::<LittleEndian>() {
                            // the start of next hit
                            if item == Self::HITS_SEPERATOR && has_hit {
                                hits.push(hit);
                                i -= 1;
                                index.word_count -= 2;
                                hit = Vec::with_capacity(4 + 4 + 4);
                            }
                            has_hit = true;
                            hit.write_u32::<LittleEndian>(item as u32).unwrap();
                            index.word_count += 1;
                        } else {
                            quit = true;
                            if !hit.is_empty() {
                                hits.push(hit);
                                index.word_count -= 2;
                            }
                            break;
                        }
                    }
                }
                index.map.insert(entry.term, hits);
            }
        }
        index.word_count /= 2;
        Ok(index)
    }

    // Search all documents that contain the term
    // and highlights where the term appears.
    pub fn search(&self, term: &str) -> io::Result<()> {
        let m: Option<&Vec<Vec<u8>>> = self.map.get(term);
        if m.is_none() {
            println!("can not found {} in all documents", term);
            return Ok(());
        }
        let hits = m.unwrap();
        for hit in hits {
            let mut cursor = Cursor::new(hit);
            let _ = cursor.read_i32::<LittleEndian>().unwrap();

            let document_id = cursor.read_u32::<LittleEndian>().unwrap();
            let doc = self.docs.get(&document_id);
            if doc.is_none() {
                println!("cannot found document {}", document_id);
                continue;
            }
            let doc = doc.unwrap();
            let mut poss = Vec::with_capacity(hits.len() / 4);
            let mut pos = TokenPos::default();
            let mut has_pos = false;
            while let Ok(p) = cursor.read_u32::<LittleEndian>() {
                if !has_pos {
                    pos.start_pos = p;
                    has_pos = true;
                } else {
                    pos.end_pos = p;
                    poss.push(pos);
                    pos = TokenPos::default();
                    has_pos = false;
                }
            }

            let result = highlight_file(doc.path.clone(), &mut poss)?;
            println!("\n{:?}: \n{}", doc.path, result);
        }
        Ok(())
    }
}

impl Default for InMemoryIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Break text into words
fn tokenize(text: &str) -> Vec<(&str, usize, usize)> {
    let mut res = Vec::new();
    let mut token_start = None;
    for (idx, ch) in text.char_indices() {
        match (ch.is_alphanumeric(), token_start) {
            // start of a word
            (true, None) => token_start = Some(idx),
            // end of a word
            (false, Some(start)) => {
                res.push((&text[start..idx], start, idx - 1));
                token_start = None
            }
            _ => {}
        }
    }

    // the last one.
    if let Some(start) = token_start {
        res.push((&text[start..], start, text.len() - 1))
    }
    res
}

fn highlight_file(path: PathBuf, poss: &mut Vec<TokenPos>) -> io::Result<String> {
    let mut origin_text = std::fs::read_to_string(path)?;
    let mut extra_chars = 0;

    // Make sure the poss is sorted by `start_pos` to prevent misalignment.
    poss.sort_by_key(|pos| pos.start_pos);

    for pos in poss.iter() {
        // Adjust the position index to add the number of additional characters that have been inserted.
        let start_pos_adjusted = (pos.start_pos as usize) + extra_chars;
        let end_pos_adjusted = (pos.end_pos as usize) + extra_chars;

        origin_text = highlight_text(&origin_text, start_pos_adjusted, end_pos_adjusted);

        extra_chars += 9; // the total length of `\x1b[31m` and `\x1b[0m`
    }

    Ok(origin_text)
}

fn highlight_text(text: &str, start_pos: usize, end_pos: usize) -> String {
    if start_pos > text.len() || end_pos >= text.len() || start_pos > end_pos {
        return text.to_string(); // Returning the original text if the positions are invalid
    }

    // Concatenating strings using format! macro for better readability
    format!(
        "{}\x1b[31m{}\x1b[0m{}",
        &text[..start_pos],         // Text before the highlight
        &text[start_pos..=end_pos], // Text to be highlighted
        &text[end_pos + 1..]        // Text after the highlight
    )
}

fn vec_to_pathbuf(bytes: Vec<u8>) -> PathBuf {
    let os_string = OsString::from_vec(bytes);
    PathBuf::from(os_string)
}
