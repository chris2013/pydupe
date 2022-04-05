# pydupe
an advanced python script to deal with dupes (a better definition of duplicates).

# Problem Statement
I have a lot of pictures on my hard disk and on my NAS flying around. Because it is from my family, I am a little bit paranoid to delete some of them by accident. Also, I make backups from e.g. my wife's computer. Usually, I am interrupted in the middle of an ongoing backup leaving the disk in an unknown state. Also, I have exported photos from my wife's Mac with different options, sometimes having the date as a title, sometimes the place. Over the time, several copies and backup of the same picture stock have accumulated on my NAS. Some of the backups have meaningful names via the export function, some are already a little bit sorted and I gave them once meaningful names. I needed help with cleaning my disk from unnecessary duplicates.

# Available software
While there are several tools out there that identify duplicates in the file system, I needed a tool for a more detailed specification of what duplicate should be selected for deletion. Also, I find the term duplicate difficult, because it implies to have an original. My problem was to specify which of the available duplicates should be the original.

# Pydupe
I ended up with this garage project to improve also my python skills. I identified the following workflow for my problem, which is supported by pydupe:

1) Recursively scan all files from one or several directories and identify all dupes. A dupe is a file which content is identical to at least one more other file. The content is regarded equal, if the hash (SHA256) of the file content is the same. One hash is mapped to a list of files with the same hash value. If the list has more than one entry, these files are called dupes.

> pydupe hash /path/to/picture/directory/


2) To dedupe a directory which might contain dupes, call pydupe dd (dedupe):

> pydupe dd /path/to/picture/directory/folder

this command first matches dupes. There are two options available to detect dupes. First, all available hashes can be taken into account, pydupe is aware of. This is the default behavior that also can be specified via the option  --dupes_global. The second option is to take only files from the specified folder (/path/to/picture/directory/folder) into account. If you want to use this behavior, give --dupes_local as option.

The identified dupes should either be kept (--match_keeps), or deleted (--match_deletions, this is the default).

The dd command takes also a pattern as argument. This is a regex pattern that is used to select dupes (for deletion or to be kept, as desired). The pattern defaults to '.' which matches everything. Specifying a pattern is useful if you want to specify in detail, what dupes should be kept, for example because the filename has meaningful repeating pattern you want to keep.

If several dupes belonging to one hash are identified for deletion, but no dupe is specified to be kept, no dupe will be deleted. However, if you specify option --autoselect, the first dupe is marked to be kept and the rest will be deleted.

3) The dd command does not alter the files on the disk unless option --do_move is given. Instead, a pretty printed tree structure is printed to the console and also stored as html output for further inspection via your fafourite web browser.

If you are sure hand want to execute the specified behavior, specify the --do_move option to alter the files on the disk. Files selected for deletion will be moved to a trash with their full absolute path appended to the trash directory path. This makes it easy to revert any changes by hand. The trash directory defaults to '~/.pydupeTrash', but can be altered with the '--trash' option.

See pydupe help for a more detaild specification of the option:
> pydupe --help

and

> pydupe dd --help

# Installation on Linux
please make sure you have a decent version of python3 installed and pip is available (I have not yet cleaned the dependencies).

1. clone this repository or download it to a directory on your computer.
2. change the working directory to the downloaded repository (contains setup.py)
3. type 
    > pip install .
4. done

# Technical
Some words to the python technics, pydupe uses:
- To identify dupes, not every file is hashed from the very beginning. Bacause necessarily all dupes need to have the same file size, just files with the same size are hashed.
- For performance reasons, Hashing is not done in pure python, but done as Threads which open shell commands as subprocesses. Therefore, the command to hash a file is system depended and specified in the config.py module. I have tested correct behavior for Linux (Ubuntu) and FreeNas (FreeBSD) by specifying more than 50 Test Cases. I have also verified pydupes works under Windows10. Using Threads really speeds up file hashing proportional to the number of Processor Cores you have.
- The hash algorithm is somewhat unimportant, I would have also chosen md5 because I have no security application. I ended up with SHA256 mainly because this is faster as it is supported by crypto hardware.
- All Hashes and File Statistics are stored in a SQLite Database (~/.sqlite as default, can be specified with --db). All database operations are done via SQL, so SQLite is not used just as a thumb storage, but the power of SQL is leveraged instead with respect to reliability and performance.
- The lookup tables are stored in an interesting data class LuTable. See module lutable.py for details. It is basically a Dictionary with hash values as keys and a list of files as values. Because just membership is important, the values should be stored as a set datatype. However, this is expensive in terms of memory usage. In addition, the advantages of using sets, fast membership testing, can be neglected for small list sizes. Instead, I have chosen to implement the values as collection.deque() objects which at least enables fast append operations. Encapsulation in a dedicated data class nicely shortens the business logic and using core class methods comply to the DRY (don't repeat yourself) principle. LuTable is neither a pure Mapping nor a pure Set. Nevertheless, I have choosen a collections.MutableMapping as foundation but added also some mapping functionality in addition. Set operations work on the file lists (implemented as deque), mapping operations on the keys.
- For console printing, I have used the 'rich' package (https://rich.readthedocs.io/en/stable/introduction.html) which really is very nice. 
- Unit Testing is done by pytest, I used it to practise TDD and it worked very well!
- The CLI Interface is implemented by using the 'click' package (https://click.palletsprojects.com/en/8.0.x/).

That's it for the moment, if I find the time I will give a detailed example. Documentation can always be improved, but unfortunately my time is limited.

Nov 7th, 2021
Christian
