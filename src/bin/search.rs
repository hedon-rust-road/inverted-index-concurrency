use std::io;

use clap::Parser;
use inverted_index_concurrency::index::InMemoryIndex;

#[derive(Parser)]
struct Opts {
    #[arg(short, long, required = true, help = "Specify index file path")]
    index_file: String,
    #[arg(short, long, required = true, help = "Specify search term")]
    term: String,
}

fn main() -> io::Result<()> {
    let opts = Opts::parse();
    let index = InMemoryIndex::from_index_file(opts.index_file)?;
    index.search(&opts.term)?;
    Ok(())
}
