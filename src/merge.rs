use std::{
    fs::{self, File},
    io::{self, BufWriter},
    mem,
    path::{Path, PathBuf},
};

use crate::{read::IndexFileReader, tmp::TmpDir, write::IndexFileWriter};

/// Represents a merging tool for combining multiple index files into a single file.
/// It uses a multi-level merging strategy to handle large numbers of files efficiently.
pub struct FileMerge {
    /// Directory where the final merged file will be stored.
    output_dir: PathBuf,

    /// Temporary directory used for intermediate files during the merge process.
    tmp_dir: TmpDir,

    /// A vector of stacks, each containing files at different levels of merging.
    /// Each inner vector represents a level in the multi-level merge process.
    stacks: Vec<Vec<PathBuf>>,
}

// How many file to merge at a time, at most.
const NSTREAMS: usize = 8;

// The final file name.
const MERGED_FILENAME: &str = "index.bat";

impl FileMerge {
    pub fn new(output_dir: &Path) -> FileMerge {
        FileMerge {
            output_dir: output_dir.to_owned(),
            tmp_dir: TmpDir::new(output_dir),
            stacks: vec![],
        }
    }

    /// Adds a file to the merge process.
    /// Files are added to a multi-level stack structure
    /// where they are progressively merged with other files.
    pub fn add_file(&mut self, mut file: PathBuf) -> io::Result<()> {
        let mut level = 0;
        loop {
            // Ensure the current level exists in the stacks vector.
            if level == self.stacks.len() {
                self.stacks.push(vec![]);
            }

            // Add the file to the current level.
            self.stacks[level].push(file);

            // Merge files at this level if the stack is full.
            if self.stacks[level].len() < NSTREAMS {
                break;
            }

            // Create a new file to store the merged result and update the stack.
            let (filename, out) = self.tmp_dir.create()?;
            let mut to_merge = vec![];
            mem::swap(&mut self.stacks[level], &mut to_merge);
            merge_streams(to_merge, out)?;
            file = filename;
            level += 1;
        }
        Ok(())
    }

    /// Completes the merge process by merging all remaining files.
    /// This method should be called after all files have been added.
    pub fn finish(mut self) -> io::Result<()> {
        let mut tmp = Vec::with_capacity(NSTREAMS);
        for stack in self.stacks {
            for file in stack.into_iter().rev() {
                tmp.push(file);
                if tmp.len() == NSTREAMS {
                    merge_reversed(&mut tmp, &mut self.tmp_dir)?;
                }
            }
        }

        if tmp.len() > 1 {
            merge_reversed(&mut tmp, &mut self.tmp_dir)?;
        }
        assert!(tmp.len() == 1);
        match tmp.pop() {
            Some(last_file) => fs::rename(last_file, self.output_dir.join(MERGED_FILENAME)),
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "no ducuments were parsed or none contained any words",
            )),
        }
    }
}

/// Merges multiple index files into a single output file.
/// This function reads through all the provided index files,
/// combines their contents based on the lexicographical order of index terms,
/// and writes the merged output to a new file.
///
/// It uses a multi-way merge algorithm, similar to that used in merge sort, to efficiently combine the files.
fn merge_streams(files: Vec<PathBuf>, out: BufWriter<File>) -> io::Result<()> {
    let mut streams: Vec<IndexFileReader> = files
        .into_iter()
        .map(|p| IndexFileReader::open_and_delete(p, true))
        .collect::<io::Result<_>>()?;

    let mut output = IndexFileWriter::new(out)?;

    let mut point: u64 = 0;
    let mut count = streams.iter().filter(|s| s.peek().is_some()).count();
    while count > 0 {
        let mut term = None;
        let mut nbytes = 0;
        let mut df = 0;
        for s in &streams {
            match s.peek() {
                None => {}
                Some(entry) => {
                    if entry.term.is_empty() {
                        term = Some(entry.term.clone());
                        nbytes = entry.nbytes;
                        df = entry.df;
                        break;
                    }
                    if term.is_none() || entry.term < *term.as_ref().unwrap() {
                        term = Some(entry.term.clone());
                        nbytes = entry.nbytes;
                        df = entry.df
                    } else if entry.term == *term.as_ref().unwrap() {
                        nbytes += entry.nbytes;
                        df += entry.df
                    }
                }
            }
        }

        let term = term.expect("bug in algorithm");

        for s in &mut streams {
            if s.is_at(&term) {
                s.move_entry_to(&mut output)?;
                if s.peek().is_none() {
                    count -= 1;
                }
                if term.is_empty() {
                    break;
                }
            }
        }

        output.write_contents_entry(term, df, point, nbytes);
        point += nbytes
    }

    Ok(())
}

/// Reverses the order of files and then merges them into one,
/// updating the original list of filenames with the result.
///
/// This function is particularly useful when a specific order of merging is required
/// that is not the natural order of the input files.
fn merge_reversed(filenames: &mut Vec<PathBuf>, tmp_dir: &mut TmpDir) -> io::Result<()> {
    filenames.reverse();
    let (merge_filename, out) = tmp_dir.create()?;
    let mut to_merge = Vec::with_capacity(NSTREAMS);
    mem::swap(filenames, &mut to_merge);
    merge_streams(to_merge, out)?;
    filenames.push(merge_filename);
    Ok(())
}
