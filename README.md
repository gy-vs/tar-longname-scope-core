# TAR archive core

TypeScript streaming TAR reader with scoped GNU `longname`/`longlink` support.

## Semantics

- GNU `L` (longname) and `K` (longlink) records are **immutable pending metadata
  bound to the next physical header**. They are consumed (and cleared) the moment
  that header is read — regardless of whether the caller accepts the entry or
  reads its body — so a filtered-out or corrupted header can never leak a long
  name onto a later entry.
- A corrupted header triggers block-boundary resynchronisation and **explicitly
  discards** extension records already bound to it.
- Each entry body is captured into a **private immutable snapshot** before the
  entry is yielded. `entry.body()` returns a fresh stream over that snapshot;
  delayed, parallel or cancelled consumption never mutates parser state.
- Zero blocks end a volume; pending metadata is dropped at the boundary, so
  concatenated archives parse as independent volumes.
- Name sequences are identical for every read schedule (1-byte chunks,
  block-aligned chunks, bodies consumed eagerly or lazily, …).

## Develop

```sh
npm install
npm test     # tsc + node --test
npm run build
```
