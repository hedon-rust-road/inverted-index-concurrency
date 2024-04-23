# Inverted Index Concurrency

This repository contains a Rust project that demonstrates the use of concurrency to build and query an inverted index efficiently. An inverted index is a data structure used extensively in search engines for storing mapping from content keywords to their locations in a document. The concurrency aspect is managed by Rust’s powerful channel to ensure optimal performance.

## Table of Contents

- [Features](#features)
- [Usage](#usage)
- [Code Overview](#code-overview)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Document Indexing**: Efficiently indexes documents by tokenizing text into words and mapping them to their respective document postions.
- **Query Search**: Allows searching for documents that contain a given word or phrase, with case-insensitivity.
- **Highlighting**: Highlights all occurrences of the search term in the returned document text in purple for easy identification.
- **Custom Tokenization**: Splits text into words based on alphanumeric boundaries, improving on traditional whitespace-based methods.

## Usage

To get started with this project, you'll need to have Rust and cargo installed on your machine. For detailed instructions, refer to the [official Rust installation guide](https://www.rust-lang.org/tools/install).

### Clone the Repository

```bash
git clone https://github.com/hedon-rust-road/inverted-index-concurrency.git
cd inverted-index-concurrency
```

### Create Index File

```bash
cargo run --bin create -- -h
```

By executing the command above, you can view the usage instructions for `create`:

```bash
Usage: create [OPTIONS] <FILENAMES>...

Arguments:
  <FILENAMES>...  

Options:
  -s, --single-threaded  Default false
  -h, --help             Print help
```

Run the following command to generate an index file using the `texts` provided in the source code:

```bash
cargo run --bin create -- ./texts
```

After execution, you should see the created `index.bat` file in the project's root directory.

### Search by Term

```bash
cargo run --bin search -- -h 
```

By executing the command above, you can view the usage instructions for `search`:

```bash
Usage: search --index-file <INDEX_FILE> --term <TERM>

Options:
  -i, --index-file <INDEX_FILE>  Specify index file path
  -t, --term <TERM>              Specify search term
  -h, --help                     Print help
```

Run the following command to build the in-memory index from the previously generated `index.bat` file and search for documents containing the word `programming` with highlighted outputs.

```bash
cargo run --bin search -- -i index.bat -t programming
```

Output example:

![search output example](https://hedonspace.oss-cn-beijing.aliyuncs.com/img/image-20240423234407840.png)

## Code Overview

This project is structured as follows:

```bash
├── src
│   ├── bin
│   │   ├── create.rs
│   │   └── search.rs
│   ├── index.rs
│   ├── lib.rs
│   ├── merge.rs
│   ├── read.rs
│   ├── tmp.rs
│   └── write.rs
└── texts
    ├── text1.txt
    ├── text2.txt
    └── text3.txt
```

The `src` directory includes several modules, each responsible for a part of the project's functionality:

- `main.rs`: Builds the index from the input files and handles the CLI.
- `index`: Manages the in-memory index data structures (`InMemoryIndex`). It includes logic for building these structures from file content and reconstructing them from index files.
- `tmp`: Manages temporary directory structures (`TmpDir`) used to store temporary index files.
- `write`: Handles writing the in-memory index to disk (`IndexFileWriter`).
- `merge`: Combines all index files in the temporary directory (`FileMerge`).
- `read`: Reads and parses the index files (`IndexFileReader`).

The project is divided into two main functionalities:

- `create`: Builds the index by specified documents.
- `search`: Implements the search functionality using the generated index file.

## Contributing

Contributions are welcome! Please fork the repository and open a pull request with your changes. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE) file for details.
