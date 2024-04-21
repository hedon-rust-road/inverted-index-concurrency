use std::{
    fs::File,
    io::{self, BufWriter},
    mem,
    path::{Path, PathBuf},
    process::Output,
};

use crate::tmp::TmpDir;

pub struct FileMerge {
    output_dir: PathBuf,
    tmp_dir: TmpDir,
    stacks: Vec<Vec<PathBuf>>,
}

const NSTREAMS: usize = 8;

const MERGED_FILENAME: &'static str = "index.bat";

impl FileMerge {
    pub fn new(output_dir: &Path) -> FileMerge {
        FileMerge {
            output_dir: output_dir.to_owned(),
            tmp_dir: TmpDir::new(output_dir.to_owned()),
            stacks: vec![],
        }
    }

    pub fn add_file(&mut self, mut file: PathBuf) -> io::Result<()> {
        let mut level = 0;
        loop {
            if level == self.stacks.len() {
                self.stacks.push(vec![]);
            }
            self.stacks[level].push(file);
            if self.stacks[level].len() < NSTREAMS {
                break;
            }
            let (filename, out) = self.tmp_dir.create()?;
            let mut to_merge = vec![];
            mem::swap(&mut self.stacks[level], &mut to_merge);
            merge_streams(to_merge, out)?;
            file = filename;
            level += 1;
        }
        Ok(())
    }
}
