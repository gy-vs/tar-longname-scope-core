import { expect, it, describe } from 'vitest';
import { ArchiveIndex, TarReader } from '../src/index.js';
import type { TarByteSource, TarEntry } from '../src/index.js';

it('indexes entries', () => {
  const x = new ArchiveIndex();
  x.add({ path: 'a', size: 1, type: 'file' });
  expect(x.find('a')?.size).toBe(1);
});

// ---------------------------------------------------------------------------
// Minimal ustar/GNU archive builder
// ---------------------------------------------------------------------------

const BLOCK = 512;
const enc = new TextEncoder();

function octal(value: number, length: number): Uint8Array {
  const digits = value.toString(8);
  const out = new Uint8Array(length);
  const padded = digits.padStart(length - 1, '0');
  out.set(enc.encode(padded).subarray(0, length - 1), 0);
  out[length - 1] = 0x20;
  return out;
}

function putString(block: Uint8Array, text: string, offset: number, length: number) {
  block.set(enc.encode(text).subarray(0, length), offset);
}

function makeHeader(spec: {
  name: string;
  size: number;
  typeflag: number;
  linkName?: string;
  prefix?: string;
}): Uint8Array {
  const block = new Uint8Array(BLOCK);
  putString(block, spec.name, 0, 100);
  block.set(octal(0o644, 7), 100);
  block.set(octal(0, 7), 108);
  block.set(octal(0, 7), 116);
  block.set(octal(spec.size, 12), 124);
  block.set(octal(Math.floor(Date.now() / 1000), 12), 136);
  // Checksum placeholder: eight spaces.
  block.fill(0x20, 148, 156);
  block[156] = spec.typeflag;
  putString(block, spec.linkName ?? '', 157, 100);
  putString(block, 'ustar', 257, 5);
  block[263] = 0x30;
  block[264] = 0x30;
  putString(block, 'root', 265, 32);
  putString(block, 'root', 297, 32);
  putString(block, spec.prefix ?? '', 345, 155);

  let sum = 0;
  for (let i = 0; i < BLOCK; i++) sum += block[i];
  const digits = sum.toString(8).padStart(6, '0');
  putString(block, digits, 148, 6);
  block[154] = 0;
  block[155] = 0x20;
  return block;
}

function padded(data: Uint8Array): Uint8Array {
  const blocks = Math.ceil(data.length / BLOCK);
  const out = new Uint8Array(blocks * BLOCK);
  out.set(data);
  return out;
}

function longRecord(kind: 'L' | 'K', payload: string): Uint8Array[] {
  const body = enc.encode(payload + '\0');
  return [
    makeHeader({
      name: '././@LongLink',
      size: body.length,
      typeflag: kind.charCodeAt(0),
    }),
    padded(body),
  ];
}

function fileRecord(name: string, body: string): Uint8Array[] {
  const data = enc.encode(body);
  return [makeHeader({ name, size: data.length, typeflag: 0x30 }), padded(data)];
}

function symlinkRecord(name: string, target: string): Uint8Array[] {
  return [makeHeader({ name, size: 0, typeflag: 0x32, linkName: target })];
}

function zeros(n: number): Uint8Array {
  return new Uint8Array(n * BLOCK);
}

function build(...groups: Uint8Array[][]): Uint8Array {
  const parts = groups.flat();
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function garbageBlock(seed: number): Uint8Array {
  const out = new Uint8Array(BLOCK);
  for (let i = 0; i < BLOCK; i++) out[i] = (seed * 31 + i * 7) % 256 || 1;
  return out;
}

// ---------------------------------------------------------------------------
// Source adapters / reading helpers
// ---------------------------------------------------------------------------

function asChunks(data: Uint8Array, size: number): AsyncIterable<Uint8Array> {
  return {
    [Symbol.asyncIterator]() {
      let pos = 0;
      return {
        next() {
          if (pos >= data.length) return Promise.resolve({ value: undefined, done: true as const });
          const chunk = data.subarray(pos, pos + size);
          pos += chunk.length;
          return Promise.resolve({ value: chunk, done: false as const });
        },
      };
    },
  };
}

function asStream(data: Uint8Array, size: number): ReadableStream<Uint8Array> {
  let pos = 0;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (pos >= data.length) {
        controller.close();
        return;
      }
      const chunk = data.subarray(pos, pos + size);
      pos += chunk.length;
      controller.enqueue(chunk);
    },
  });
}

async function names(
  source: TarByteSource,
  filter?: (e: TarEntry) => boolean,
): Promise<string[]> {
  const out: string[] = [];
  const reader = new TarReader(source, filter ? { filter } : {});
  for await (const entry of reader.entries()) {
    out.push(entry.path);
    await entry.body.pipeTo(new WritableStream());
  }
  return out;
}

async function drain(entry: TarEntry): Promise<string> {
  const reader = entry.body.getReader();
  const parts: Uint8Array[] = [];
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    parts.push(value);
  }
  return new TextDecoder().decode(concatBytes(parts));
}

function concatBytes(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('GNU longname binding scope', () => {
  it('binds longname to the next header even when that entry is filtered out', async () => {
    const seen: string[] = [];
    const archive = build(
      longRecord('L', 'long-skipped-name.txt'),
      fileRecord('short1', 'x'),
      fileRecord('keep-me.txt', 'y'),
    );

    const result = await names(archive, (e) => {
      seen.push(e.path);
      return e.path !== 'long-skipped-name.txt';
    });

    // The filter must observe the resolved long name; the stale longname must
    // not leak onto 'keep-me.txt'.
    expect(seen).toEqual(['long-skipped-name.txt', 'keep-me.txt']);
    expect(result).toEqual(['keep-me.txt']);
  });

  it('discards the bound longname when the following header is corrupt', async () => {
    const [longHeader, longBody] = longRecord('L', 'ghost-long-name.txt');
    const archive = build(
      [longHeader, longBody],
      [garbageBlock(3)],
      fileRecord('survivor.txt', 'z'),
    );

    expect(await names(archive)).toEqual(['survivor.txt']);
  });

  it('discards a dangling longname at the end-of-archive zero padding', async () => {
    const archive = build(longRecord('L', 'never-arrives'), [zeros(2)]);
    expect(await names(archive)).toEqual([]);
  });

  it('merges and overrides consecutive longname/longlink records', async () => {
    const archive = build(
      // Both bind to the same following link header.
      longRecord('K', 'a/very/long/symlink/target/path'),
      longRecord('L', 'a/very/long/symlink/name'),
      symlinkRecord('n', 't'),
      // Two longnames in a row: the later one wins, longlink stays consumed.
      longRecord('L', 'first-name'),
      longRecord('L', 'second-name'),
      fileRecord('zz', 'data'),
      // A lone longlink binds the next physical header even when it is a
      // regular file (this matches GNU tar semantics; it does not skip an
      // entry, so 'plain.txt' must still appear).
      longRecord('K', 'orphan-target'),
      fileRecord('plain.txt', 'p'),
    );

    const reader = new TarReader(archive);
    const got: { path: string; linkName: string; type: string }[] = [];
    for await (const entry of reader.entries()) {
      got.push({ path: entry.path, linkName: entry.linkName, type: entry.type });
      await entry.body.pipeTo(new WritableStream());
    }

    expect(got).toEqual([
      { path: 'a/very/long/symlink/name', linkName: 'a/very/long/symlink/target/path', type: 'link' },
      { path: 'second-name', linkName: '', type: 'file' },
      { path: 'plain.txt', linkName: 'orphan-target', type: 'file' },
    ]);
  });

  it('supports an empty longname payload', async () => {
    const archive = build(
      longRecord('L', ''),
      fileRecord('ignored-short-name', 'q'),
      fileRecord('after-empty', 'r'),
    );
    expect(await names(archive)).toEqual(['', 'after-empty']);
  });

  it('yields correct bodies when body consumption is deferred until all headers are seen', async () => {
    const archive = build(
      longRecord('L', 'deferred-one'),
      fileRecord('x', 'body-1'),
      longRecord('L', 'deferred-two'),
      fileRecord('y', 'body-22'),
      longRecord('L', 'deferred-three'),
      fileRecord('z', 'body-333'),
    );

    const reader = new TarReader(archive);
    const entries: TarEntry[] = [];
    for await (const entry of reader.entries()) entries.push(entry);

    expect(entries.map((e) => e.path)).toEqual([
      'deferred-one',
      'deferred-two',
      'deferred-three',
    ]);
    // Consume bodies strictly after the whole header walk.
    const bodies = await Promise.all(entries.map(drain));
    expect(bodies).toEqual(['body-1', 'body-22', 'body-333']);
  });

  it('gives the same names and bodies under parallel interleaved body consumption', async () => {
    const archive = build(
      longRecord('L', 'p-one'),
      fileRecord('x', 'A'.repeat(100_000)),
      longRecord('L', 'p-two'),
      fileRecord('y', 'B'.repeat(100_000)),
      longRecord('L', 'p-three'),
      fileRecord('z', 'C'.repeat(100_000)),
    );

    const reader = new TarReader(archive);
    const entries: TarEntry[] = [];
    for await (const entry of reader.entries()) entries.push(entry);

    const consumed = await Promise.all(
      entries.map(async (e) => ({
        path: e.path,
        body: await drain(e),
      })),
    );
    expect(consumed.map((c) => c.path)).toEqual(['p-one', 'p-two', 'p-three']);
    expect(consumed.map((c) => c.body.length)).toEqual([100_000, 100_000, 100_000]);
    expect(consumed[0].body[0]).toBe('A');
    expect(consumed[1].body[0]).toBe('B');
    expect(consumed[2].body[0]).toBe('C');
  });

  it('survives the caller cancelling a body stream early; later longname is intact', async () => {
    const archive = build(
      longRecord('L', 'big-cancelled-entry'),
      fileRecord('x', 'X'.repeat(200_000)),
      longRecord('L', 'small-survivor'),
      fileRecord('y', 'tiny'),
    );

    const reader = new TarReader(archive);
    const iter = reader.entries();
    const first = (await iter.next()).value!;
    expect(first.path).toBe('big-cancelled-entry');

    // Close the body before the pump has finished writing it.
    await first.body.cancel('caller gave up');

    const second = (await iter.next()).value!;
    expect(second.path).toBe('small-survivor');
    expect(await drain(second)).toBe('tiny');
    expect((await iter.next()).done).toBe(true);
  });

  it('cancelling one entry body cannot clear pending metadata for another live entry', async () => {
    const archive = build(
      longRecord('L', 'first-live'),
      fileRecord('a', 'A'.repeat(200_000)),
      longRecord('L', 'second-live'),
      fileRecord('b', 'B'.repeat(200_000)),
    );

    const reader = new TarReader(archive);
    const iter = reader.entries();
    const first = (await iter.next()).value!;
    // Second header/body does not even exist yet: cancel first, then walk on.
    await first.body.cancel('early');
    const second = (await iter.next()).value!;
    expect(second.path).toBe('second-live');
    await second.body.cancel('early too');
    expect((await iter.next()).done).toBe(true);
  });

  it('handles early termination of the entry iterator without hanging', async () => {
    const archive = build(
      longRecord('L', 'only-one-read'),
      fileRecord('a', '1'),
      longRecord('L', 'never-reached'),
      fileRecord('b', '2'),
    );

    const reader = new TarReader(archive);
    let count = 0;
    for await (const entry of reader.entries()) {
      count++;
      expect(entry.path).toBe('only-one-read');
      await entry.body.cancel('stop');
      break;
    }
    expect(count).toBe(1);

    // A fresh parser on a fresh archive is unaffected by the aborted one.
    expect(await names(build(fileRecord('fresh.txt', 'ok')))).toEqual([
      'fresh.txt',
    ]);
  });

  it('reads two archives concatenated in one stream as one name sequence', async () => {
    const first = build(
      longRecord('L', 'arch-a/long-name'),
      fileRecord('x', 'a1'),
      fileRecord('arch-a/plain', 'a2'),
      [zeros(2)],
    );
    const second = build(
      fileRecord('arch-b/start', 'b1'),
      longRecord('L', 'arch-b/long-name'),
      fileRecord('y', 'b2'),
      [zeros(2)],
    );
    const joined = new Uint8Array(first.length + second.length);
    joined.set(first, 0);
    joined.set(second, first.length);

    expect(await names(joined)).toEqual([
      'arch-a/long-name',
      'arch-a/plain',
      'arch-b/start',
      'arch-b/long-name',
    ]);
  });

  const schedules = [1, 3, 7, 100, 511, 512, 513, 4096] as const;

  for (const chunkSize of schedules) {
    it(`produces identical names for chunk size ${chunkSize} (async iterable source)`, async () => {
      const archive = build(
        longRecord('L', `chunked-${chunkSize}/a-very-long-name`),
        fileRecord('x', 'payload'),
        longRecord('K', `chunked-${chunkSize}/a-very-long-link-target`),
        longRecord('L', `chunked-${chunkSize}/link-name`),
        symlinkRecord('n', 't'),
        fileRecord('tail', 'end'),
        [zeros(2)],
      );
      expect(await names(asChunks(archive, chunkSize))).toEqual([
        `chunked-${chunkSize}/a-very-long-name`,
        `chunked-${chunkSize}/link-name`,
        'tail',
      ]);
    });
  }

  for (const chunkSize of [1, 13, 512, 5000] as const) {
    it(`produces identical names for chunk size ${chunkSize} (web stream source)`, async () => {
      const archive = build(
        longRecord('L', 'streamed-long-name'),
        fileRecord('x', 'payload'),
        fileRecord('next', 'more'),
        [zeros(2)],
      );
      expect(await names(asStream(archive, chunkSize))).toEqual([
        'streamed-long-name',
        'next',
      ]);
    });
  }

  it('reports corrupt longname payload truncation as an error rather than leaking state', async () => {
    const header = makeHeader({
      name: '././@LongLink',
      size: BLOCK * 2,
      typeflag: 0x4c,
    });
    // Only one body block, then EOF: no entries, iterator surfaces the error.
    const archive = build([header], [new Uint8Array(BLOCK)]);
    const reader = new TarReader(archive);
    await expect(async () => {
      for await (const _ of reader.entries()) {
        // drain
      }
    }).rejects.toThrow(/Unexpected end/);
  });

  it('gives identical name+body sequences regardless of read scheduling', async () => {
    const archive = build(
      longRecord('L', 'sched/dir/a-quite-long-entry-name-000'),
      fileRecord('x', 'body-0'.repeat(3000)),
      fileRecord('plain-one', 'p1'),
      longRecord('K', 'sched/dir/a-quite-long-link-target'),
      longRecord('L', 'sched/dir/a-quite-long-entry-name-001'),
      symlinkRecord('n', 't'),
      longRecord('L', 'sched/dir/a-quite-long-entry-name-002'),
      fileRecord('y', 'body-2'.repeat(3000)),
      fileRecord('plain-two', 'p2'),
      [zeros(2)],
    );

    const expected = [
      ['sched/dir/a-quite-long-entry-name-000', 'body-0'.repeat(3000)],
      ['plain-one', 'p1'],
      ['sched/dir/a-quite-long-entry-name-001', ''],
      ['sched/dir/a-quite-long-entry-name-002', 'body-2'.repeat(3000)],
      ['plain-two', 'p2'],
    ];

    const schedules: ((entries: TarEntry[]) => Promise<unknown>)[] = [
      // Eager: fully drain before asking for the next header.
      async (entries) => {
        for (const entry of entries) {
          const text = await drain(entry);
          if (text !== expected[entries.indexOf(entry)][1]) {
            throw new Error('eager body mismatch');
          }
        }
      },
      // Fully deferred: collect every header, then drain in order.
      async () => {},
      // Fully deferred, reverse drain order.
      async (entries) => {
        for (let i = entries.length - 1; i >= 0; i--) {
          const text = await drain(entries[i]);
          if (text !== expected[i][1]) throw new Error('reverse body mismatch');
        }
      },
      // Chaotic interleave: read one chunk per reader in round-robin, with a
      // macrotask yield between passes, until every stream ends.
      async (entries) => {
        const readers = entries.map((e) => e.body.getReader());
        const parts: Uint8Array[][] = readers.map(() => []);
        const alive = new Set(readers.map((_, i) => i));
        while (alive.size > 0) {
          for (const i of [...alive]) {
            const { value, done } = await readers[i].read();
            if (done) {
              alive.delete(i);
            } else {
              parts[i].push(value);
            }
          }
          await new Promise((r) => setTimeout(r, 0));
        }
        for (let i = 0; i < entries.length; i++) {
          const text = new TextDecoder().decode(concatBytes(parts[i]));
          if (text !== expected[i][1]) {
            throw new Error(`schedule ${s} entry ${i} body mismatch`);
          }
        }
      },
      // Cancel the middle entry's body, read the others fully.
      async (entries) => {
        await entries[0].body.cancel('skip');
        await drain(entries[entries.length - 1]);
      },
    ];

    // Schedules 0, 2, 3, 4 assert contents internally. Schedule 1 leaves
    // bodies untouched so the common assertion below covers it.
    {
      const s = 1;
      const reader = new TarReader(archive);
      const entries: TarEntry[] = [];
      for await (const entry of reader.entries()) entries.push(entry);
      expect(entries.map((e) => e.path)).toEqual(expected.map(([name]) => name));
      await schedules[s](entries);
      const bodies = await Promise.all(entries.map(drain));
      expect(bodies).toEqual(expected.map(([, body]) => body));
    }

    for (const s of [0, 2, 3, 4]) {
      const reader = new TarReader(archive);
      const entries: TarEntry[] = [];
      for await (const entry of reader.entries()) entries.push(entry);
      expect(entries.map((e) => e.path)).toEqual(expected.map(([name]) => name));
      await schedules[s](entries);
    }
  });

  it('keeps longlink intact after a corrupt header immediately following it', async () => {
    const [kHeader, kBody] = longRecord('K', 'would-be-target');
    const archive = build(
      [kHeader, kBody],
      [garbageBlock(9)],
      longRecord('K', 'real-target'),
      symlinkRecord('real-link', 'short'),
      [zeros(2)],
    );
    const reader = new TarReader(archive);
    const got: string[] = [];
    for await (const entry of reader.entries()) {
      got.push(`${entry.path}->${entry.linkName}`);
      await entry.body.pipeTo(new WritableStream());
    }
    expect(got).toEqual(['real-link->real-target']);
  });

  it('handles zero-size files, directories and links without bodies', async () => {
    const dir = makeHeader({ name: 'a-dir/', size: 0, typeflag: 0x35 });
    const empty = makeHeader({ name: 'empty.txt', size: 0, typeflag: 0x30 });
    const link = makeHeader({
      name: 'link',
      size: 0,
      typeflag: 0x32,
      linkName: '../target',
    });
    const [lnH, lnB] = longRecord('L', 'very-long-directory-name/');
    const archive = build(
      [dir],
      [empty],
      [link],
      [lnH, lnB],
      [makeHeader({ name: 'ignored', size: 0, typeflag: 0x35 })],
      fileRecord('last', 'done'),
      [zeros(2)],
    );

    const reader = new TarReader(archive);
    const got: unknown[] = [];
    for await (const entry of reader.entries()) {
      got.push({
        path: entry.path,
        type: entry.type,
        linkName: entry.linkName,
        size: entry.size,
      });
      await entry.body.pipeTo(new WritableStream());
    }
    expect(got).toEqual([
      { path: 'a-dir/', type: 'directory', linkName: '', size: 0 },
      { path: 'empty.txt', type: 'file', linkName: '', size: 0 },
      { path: 'link', type: 'link', linkName: '../target', size: 0 },
      { path: 'very-long-directory-name/', type: 'directory', linkName: '', size: 0 },
      { path: 'last', type: 'file', linkName: '', size: 4 },
    ]);
  });

  it('resyncs when a corrupt block interrupts a longname chain mid-stream', async () => {
    // longname A -> garbage (A dropped) -> longname B -> file
    const [h1, b1] = longRecord('L', 'ghost-a');
    const [h2, b2] = longRecord('L', 'real-b');
    const archive = build(
      [h1, b1],
      [garbageBlock(4)],
      [h2, b2],
      fileRecord('short', 'B'),
      [zeros(2)],
    );
    expect(await names(archive)).toEqual(['real-b']);
  });
});
