use std::{
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::mpsc::{channel, Receiver},
    thread::{spawn, JoinHandle},
};

use inverted_index_concurrency::{
    index::InMemoryIndex, tmp::TmpDir, write::write_index_to_tmp_file,
};

/// Start a thread that loads documents from the filesystem into memory.
///
/// `documents` is a list of filenames to load.
///
/// This returns a pair of values: a receiver that receives the documents'
/// path and content; and a `JoinHandle` that can be used to wait for this
/// thread to exit and to get the `io::Error` value if anything goes wrong.
fn start_file_reader_thread(
    documents: Vec<PathBuf>,
) -> (Receiver<(PathBuf, String)>, JoinHandle<io::Result<()>>) {
    let (sender, receiver) = channel();

    let handler = spawn(move || {
        for filename in documents {
            let mut f = File::open(filename.clone())?;
            let mut text = String::new();
            f.read_to_string(&mut text)?;
            if sender.send((filename, text)).is_err() {
                break;
            }
        }
        Ok(())
    });

    (receiver, handler)
}

/// Start a thread that tokenizes each text and converts it into an in-memory
/// index. (We assume that every document fits comfortably in memory.)
///
/// `docs` is the stream of documents from the file reader thread.
///
/// This assigns each document a number. It returns a pair of values: a
/// receiver, the sequence of in-memory indexes; and a `JoinHandle` that can be
/// used to wait for this thread to exit. This stage of the pipeline is
/// infallible (it performs no I/O, so there are no possible errors).
fn start_file_indexing_thread(
    docs: Receiver<(PathBuf, String)>,
) -> (Receiver<InMemoryIndex>, JoinHandle<()>) {
    let (sender, receiver) = channel();

    let handler = spawn(move || {
        for (doc_id, (path, text)) in docs.into_iter().enumerate() {
            let index = InMemoryIndex::from_single_document(doc_id as u32, path, text);
            if sender.send(index).is_err() {
                break;
            }
        }
    });

    (receiver, handler)
}

/// Start a thread that merges in-memory indexes.
///
/// `file_indexes` receives a stream of indexes from the file indexing thread.
/// These indexes typically vary a lot in size, since the input documents will
/// typically be all different sizes.
///
/// The thread created by this function merges those indexes into "large"
/// indexes and passes these large indexes on to a new channel.
///
/// This returns a pair: a receiver, the sequence of large indexes produced by
/// merging the input indexes; and a `JoinHandle` that can be used to wait for
/// this thread to exit. This stage of the pipeline is infallible (it performs
/// no I/O).
fn start_in_memory_merge_thread(
    indexes: Receiver<InMemoryIndex>,
) -> (Receiver<InMemoryIndex>, JoinHandle<()>) {
    let (sender, receiver) = channel();

    let handle = spawn(move || {
        let mut accumulated_index = InMemoryIndex::new();
        for i in indexes {
            accumulated_index.merge(i);
            if accumulated_index.is_large() {
                if sender.send(accumulated_index).is_err() {
                    return;
                }
                accumulated_index = InMemoryIndex::new();
            }
        }
        if !accumulated_index.is_empty() {
            let _ = sender.send(accumulated_index);
        }
    });

    (receiver, handle)
}

/// Start a thread that saves large indexes to temporary files.
///
/// This thread generates a meaningless unique filename for each index in
/// `big_indexes`, saves the data, and passes the filename on to a new channel.
///
/// This returns a pair: a receiver that receives the filenames; and a
/// `JoinHandle` that can be used to wait for this thread to exit and receive
/// any I/O errors it encountered.
fn start_index_writer_thread(
    big_indexes: Receiver<InMemoryIndex>,
    output_dir: &Path,
) -> (Receiver<PathBuf>, JoinHandle<io::Result<()>>) {
    let (sender, receiver) = channel();

    let mut tmp_dir = TmpDir::new(output_dir);
    let handle = spawn(move || {
        for i in big_indexes {
            let file = write_index_to_tmp_file(i, &mut tmp_dir)?;
            if sender.send(file).is_err() {
                break;
            }
        }
        Ok(())
    });

    (receiver, handle)
}

fn main() {}
