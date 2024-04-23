use std::io;

use inverted_index_concurrency::index::InMemoryIndex;

fn main() -> io::Result<()> {
    let filename = "index.bat";
    let index = InMemoryIndex::from_index_file(filename)?;
    index.search("programming")?;
    Ok(())
}
