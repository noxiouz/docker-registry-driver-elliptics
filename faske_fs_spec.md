## Description of fake filesystem based on Elliptics

Each file is stored by fullpath key and is tagged by its parent directory to support directory listing.
For example, let's glance at `a/b/c`. Data of thic `c` is stored by key `a/b/c`.
And this key is tagged by index `a/b`. Also fake files would be created for imitating directory tree `a` and `a/b`.
Both of them consist of `DIRECTORY` fake data, as Elliptics doesn't support zero-sized files. And, of cource, both of them are tagged by indexes: `a` by "", `a/b` by `a`.
So to perform listing of any directory (i.e `a/b`) we should find all keys, which have been tagged by `a/b`.
When key is going to be removed corresponding tages would be removed as well.

## Example

Imagine that `a/b/c` is stored. Its content is 'MYDATA'.


KEY    | TAGS     | CONTENT
-------|----------|----------
`a`    | ``       | DIRECTORY
`a/b`  | `a`      | DIRECTORY
`a/b/c`| `a/b`    | MYDATA

