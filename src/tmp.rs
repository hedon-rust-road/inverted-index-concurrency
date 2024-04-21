use std::{
    fs::{self, File},
    io::{self, BufWriter},
    path::{Path, PathBuf},
};

/// Represents a temporary directory where temporary files can be created.
#[derive(Clone)]
pub struct TmpDir {
    /// The path to the temporary directory.
    dir: PathBuf,
    /// A counter used to generate unique file names within the temporary directory.
    n: usize,
}

impl TmpDir {
    /// Creates a new `TmpDir` instance for managing temporary files in a specified directory.
    ///
    /// # Arguments
    ///
    /// * `dir` - A path to the directory where the temporary files will be created.
    ///
    /// # Returns
    ///
    /// * A new instance of `TmpDir`.
    pub fn new<P: AsRef<Path>>(dir: P) -> TmpDir {
        TmpDir {
            dir: dir.as_ref().to_owned(),
            n: 1,
        }
    }

    /// Attempts to create a new temporary file in the directory.
    ///
    /// This method will try different filenames to avoid collisions, using a simple numeric increment.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the file could not be created which could be due to
    /// permissions issues or because the file already exists after many attempts.
    ///
    /// # Returns
    ///
    /// * On success, returns a tuple containing the `PathBuf` of the newly created file and a `BufWriter<File>` for writing to the file.
    /// * On failure, returns an `io::Error`.
    pub fn create(&mut self) -> io::Result<(PathBuf, BufWriter<File>)> {
        let mut r#try = 1;
        loop {
            let filename = self
                .dir
                .join(PathBuf::from(format!("tmp{:08x}.dat", self.n)));
            self.n += 1;
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&filename)
            {
                Ok(f) => return Ok((filename, BufWriter::new(f))),
                Err(exc) => {
                    if r#try < 999 && exc.kind() == io::ErrorKind::AlreadyExists {
                        // keep going
                    } else {
                        return Err(exc);
                    }
                }
            }
            r#try += 1;
        }
    }
}
