// TAR archive reader core.
//
// GNU longname ('L') / longlink ('K') entries are treated as *immutable
// pending metadata* bound to the next physical header: they are consumed the
// instant that header is read, regardless of whether the caller accepts the
// resulting entry (filters), reads its body, or cancels the body stream.
// Entry body streams hold a private snapshot of their own entry and keep no
// reference to the parser, so late closes or parallel body consumption can
// never clear parser state that belongs to a subsequent entry.

export type TarType = 'file' | 'directory' | 'link';

export type TarHeader = {
  path: string;
  size: number;
  type: TarType;
};

export function mergeMetadata(
  header: TarHeader,
  globalPax: Record<string, string>,
  localPax: Record<string, string>,
  longname?: string,
): TarHeader {
  return {
    ...header,
    ...localPax,
    ...globalPax,
    path: globalPax.path ?? localPax.path ?? longname ?? header.path,
    size: Number(localPax.size ?? globalPax.size ?? header.size),
  };
}

export class ArchiveIndex {
  #entries: TarHeader[] = [];
  add(entry: TarHeader) {
    this.#entries.push(entry);
  }
  list() {
    return this.#entries.slice();
  }
  find(path: string) {
    return this.#entries.find((entry) => entry.path === path);
  }
}

export type TarEntry = {
  readonly path: string;
  readonly linkName: string;
  readonly size: number;
  readonly type: TarType;
  /** Body stream for this entry only; cancelling it cannot touch the parser. */
  readonly body: ReadableStream<Uint8Array>;
};

export type TarFilter = (entry: {
  path: string;
  linkName: string;
  size: number;
  type: TarType;
}) => boolean;

export type TarReadOptions = {
  /** Entries for which this returns false are skipped (body drained). */
  filter?: TarFilter;
};

export type TarByteSource =
  | Uint8Array
  | ReadableStream<Uint8Array>
  | AsyncIterable<Uint8Array>;

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

const BLOCK = 512;
const BODY_HWM = 64 * 1024;

const decoder = new TextDecoder();

// GNU longname ('L') / longlink ('K') payload bound to the next physical
// header. Frozen snapshots only: the parser owns the sole reference and
// replaces/discards it as a whole. Body streams never see this object.
type PendingMeta = {
  readonly longname?: string;
  readonly longlink?: string;
};

function toAsyncIterator(source: TarByteSource): AsyncIterator<Uint8Array> {
  if (source instanceof Uint8Array) {
    let done = false;
    return {
      next() {
        if (done) return Promise.resolve({ value: undefined, done: true });
        done = true;
        return Promise.resolve({ value: source, done: false });
      },
    };
  }
  if (typeof (source as ReadableStream<Uint8Array>).getReader === 'function') {
    const reader = (source as ReadableStream<Uint8Array>).getReader();
    return {
      next: () => reader.read(),
      return: async () => {
        await reader.cancel();
        return { value: undefined, done: true as const };
      },
    };
  }
  return (source as AsyncIterable<Uint8Array>)[Symbol.asyncIterator]();
}

/**
 * Body channel owned by exactly one entry. It deliberately holds no
 * reference to the parser: cancel/close/error mutate only this channel, so a
 * late consumer close cannot clear parser state for any later entry.
 *
 * The channel is an unbounded async queue, fully decoupling the parser pump
 * from consumption: the pump never blocks waiting for a consumer, so headers
 * keep flowing whether bodies are consumed eagerly, late, in parallel, or
 * never. Callers that need bounded memory should consume entries promptly;
 * body payloads are retained only until read.
 */
class BodyChannel {
  #stream: ReadableStream<Uint8Array>;
  #controller!: ReadableStreamDefaultController<Uint8Array>;
  #canceled = false;

  constructor() {
    this.#stream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.#controller = controller;
      },
      cancel: () => {
        // Caller abandoned THIS entry; parser state is untouched.
        this.#canceled = true;
      },
    },
    {
      // Advisory buffering bound: the pump does not honour backpressure (it
      // must keep scanning headers), but this still reports queue pressure to
      // consumers inspecting desiredSize.
      highWaterMark: BODY_HWM,
      size: (chunk: Uint8Array) => chunk.length,
    },
  );
  }

  get stream() {
    return this.#stream;
  }
  get canceled() {
    return this.#canceled;
  }

  /**
   * Push one chunk. Never blocks the pump: the stream's internal queue
   * buffers it until the consumer reads it. Returns false after cancellation
   * so the pump can switch to draining the remaining physical blocks.
   */
  write(chunk: Uint8Array): boolean {
    if (this.#canceled) return false;
    try {
      this.#controller.enqueue(chunk);
    } catch {
      // Stream was closed/errored under us (e.g. consumer cancel raced).
      this.#canceled = true;
      return false;
    }
    return true;
  }

  close() {
    if (this.#canceled) return;
    this.#canceled = true; // terminal; further writes become no-ops
    try {
      this.#controller.close();
    } catch {
      // already closed
    }
  }

  fail(error: Error) {
    if (this.#canceled) return;
    this.#canceled = true;
    try {
      this.#controller.error(error);
    } catch {
      // already closed
    }
  }
}

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: Error) => void;
};

function defer<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

export class TarReader {
  #source: AsyncIterator<Uint8Array>;
  #filter: TarFilter | undefined;
  #buffered: Uint8Array[] = [];
  #bufferedBytes = 0;

  #pumpStarted = false;
  #aborted = false;
  #finished = false;
  #pumpDone: Promise<void> | undefined;
  #error: Error | undefined;

  #entryQueue: TarEntry[] = [];
  #entryWaiters: Deferred<IteratorResult<TarEntry>>[] = [];

  #liveChannels = new Set<BodyChannel>();
  // Entries delivered but not yet fully pumped, plus one slot for the header
  // currently being scanned. Caps how far ahead of the consumer we run.
  #inFlight = 0;
  #maxInFlight = 4;
  #bodySlots: Deferred<void>[] = [];

  // Reading happens in strictly physical sections: the header scanner owns
  // the source until it has produced the next entry, then that entry's body
  // pump owns it until every body block is drained (even when the caller
  // cancelled the stream). Ownership then returns to the scanner.
  #sectionGate: Promise<unknown> = Promise.resolve();

  // Immutable metadata bound to the next physical header.
  #pending: PendingMeta | null = null;

  constructor(source: TarByteSource, options: TarReadOptions = {}) {
    this.#source = toAsyncIterator(source);
    this.#filter = options.filter;
  }

  entries(): AsyncIterableIterator<TarEntry> {
    if (!this.#pumpStarted) {
      this.#pumpStarted = true;
      this.#pumpDone = this.#runPump();
    }
    const reader = this;
    return {
      [Symbol.asyncIterator]() {
        return this;
      },
      next() {
        return reader.#nextEntry();
      },
      return(value?: unknown) {
        return reader.#abort(value);
      },
    };
  }

  #nextEntry(): Promise<IteratorResult<TarEntry>> {
    const queued = this.#entryQueue.shift();
    if (queued && !this.#error) {
      // The consumer has taken one delivered entry: free a look-ahead slot.
      this.#releaseSlot();
      return Promise.resolve({ value: queued, done: false });
    }
    if (queued && this.#error) {
      // A fatal error arrived after this entry was queued; requeue and fail.
      this.#entryQueue.unshift(queued);
      return Promise.reject(this.#error);
    }
    if (this.#finished) {
      if (this.#error) return Promise.reject(this.#error);
      return Promise.resolve({ value: undefined, done: true });
    }
    const d = defer<IteratorResult<TarEntry>>();
    this.#entryWaiters.push(d);
    return d.promise;
  }

  async #abort(value?: unknown): Promise<IteratorResult<TarEntry>> {
    this.#aborted = true;
    this.#finished = true;
    for (const channel of this.#liveChannels) {
      channel.fail(new Error('TarReader cancelled'));
    }
    this.#liveChannels.clear();
    // Unblock the header scanner if it is waiting for a delivery slot.
    for (const slot of this.#bodySlots.splice(0)) slot.resolve();
    const result: IteratorResult<TarEntry> = {
      value: value as TarEntry | undefined,
      done: true,
    };
    for (const waiter of this.#entryWaiters.splice(0)) waiter.resolve(result);
    try {
      await this.#source.return?.();
    } catch {
      // best effort
    }
    await this.#pumpDone?.catch(() => {});
    return result;
  }

  async #runPump(): Promise<void> {
    const bodyJobs: Promise<void>[] = [];
    let fatal: Error | undefined;
    try {
      while (!this.#aborted) {
        // Reserve a delivery slot BEFORE touching the source, bounding how
        // far ahead of the consumer the scanner runs.
        await this.#acquireSlot();
        if (this.#aborted) break;

        // Exclusive header section: read zero/extension/regular headers and
        // their inline payloads until the next deliverable entry (or EOF).
        const outcome = await this.#runExclusive(() => this.#scanHeaders());

        if (outcome.kind === 'eof') {
          this.#releaseSlot();
          break;
        }

        if (outcome.kind === 'skip') {
          this.#releaseSlot();
          // Body belongs to the same logical section as the header: drain it
          // before the scanner resumes.
          await this.#runExclusive(() => this.#drainBody(outcome.size));
          continue;
        }

        const { channel, entry, size } = outcome;
        this.#deliver(entry);

        // Hand the exclusive source section over to the body pump. Its job is
        // chained onto #sectionGate, so the next #scanHeaders waits until the
        // physical body (including padding) is fully drained.
        const job = this.#runExclusive(async () => {
          try {
            await this.#pumpBody(channel, size);
          } finally {
            this.#liveChannels.delete(channel);
          }
        });
        bodyJobs.push(
          job.catch((error) => {
            if (!this.#aborted && !fatal) fatal = error as Error;
          }),
        );
      }
    } catch (error) {
      fatal = error as Error;
    }

    // All body sections (chained on #sectionGate) must finish first; pumps
    // keep draining even after their consumer cancels a stream.
    await Promise.allSettled(bodyJobs);

    this.#finished = true;
    if (this.#aborted) {
      // Cancellation is not an error; waiters were settled by #abort.
      return;
    }
    if (fatal) {
      this.#error = fatal;
      this.#failAll(fatal);
      for (const waiter of this.#entryWaiters.splice(0)) waiter.reject(fatal);
    } else {
      for (const waiter of this.#entryWaiters.splice(0)) {
        waiter.resolve({ value: undefined, done: true });
      }
    }
  }

  /**
   * One exclusive pass over header blocks. Extension records (GNU L/K) are
   * read inline and rebound here; a regular entry is returned for body
   * handoff; a filtered entry is returned as a 'skip' with its size; zero
   * padding is consumed (allowing concatenated archives) until EOF.
   */
  async #scanHeaders(): Promise<
    | { kind: 'eof' }
    | { kind: 'skip'; size: number }
    | { kind: 'entry'; channel: BodyChannel; entry: TarEntry; size: number }
  > {
    while (!this.#aborted) {
      const block = await this.#nextHeaderBlock();
      if (!block) return { kind: 'eof' };

      // The physical header is in hand: consume whatever was bound to it
      // IMMEDIATELY and detach it — before any filtering or body handling.
      // #nextHeaderBlock has already reset #pending when it skipped a corrupt
      // header or zero padding, so stale metadata can never reach a later
      // entry or a concatenated archive.
      const bound: PendingMeta | null = this.#pending;
      this.#pending = null;

      const header = parseHeader(block);

      if (header.kind !== 'entry') {
        const text = await this.#readStringBody(header.size);
        const payload = stripTrailingNuls(text);
        // Rebind onto the next physical header as a new immutable snapshot.
        const next: PendingMeta =
          header.kind === 'longname'
            ? { ...(bound ?? {}), longname: payload }
            : { ...(bound ?? {}), longlink: payload };
        this.#pending = Object.freeze(next);
        continue;
      }

      // Resolve names from the immutable snapshot captured at this header.
      // `undefined` (no extension record) differs from an empty longname
      // payload, which legitimately yields an empty name.
      const path = bound?.longname !== undefined ? bound.longname : header.path;
      const linkName =
        bound?.longlink !== undefined ? bound.longlink : header.linkName;
      const meta = {
        path,
        linkName,
        size: header.size,
        type: header.type,
      };

      if (this.#filter && !this.#filter({ ...meta })) {
        // Extension metadata was already detached above; it cannot spill onto
        // the entry after this one.
        return { kind: 'skip', size: header.size };
      }

      const channel = new BodyChannel();
      this.#liveChannels.add(channel);
      // Frozen snapshot: the body stream can never mutate this entry.
      const entry: TarEntry = Object.freeze({
        path: meta.path,
        linkName: meta.linkName,
        size: meta.size,
        type: meta.type,
        body: channel.stream,
      });
      return { kind: 'entry', channel, entry, size: header.size };
    }
    return { kind: 'eof' };
  }

  #failAll(error: Error) {
    for (const channel of this.#liveChannels) channel.fail(error);
    this.#liveChannels.clear();
  }

  #acquireSlot(): Promise<void> {
    if (this.#aborted) return Promise.resolve();
    if (this.#inFlight < this.#maxInFlight) {
      this.#inFlight++;
      return Promise.resolve();
    }
    const d = defer<void>();
    this.#bodySlots.push(d);
    return d.promise;
  }

  #releaseSlot() {
    this.#inFlight--;
    const next = this.#bodySlots.shift();
    if (next) {
      this.#inFlight++;
      next.resolve();
    }
  }

  async #pumpBody(channel: BodyChannel, size: number): Promise<void> {
    let remaining = size;
    while (remaining > 0) {
      const chunk = await this.#bodyBlock(remaining);
      remaining -= chunk.length;
      if (channel.canceled) {
        // Caller closed early: the parser still advances over this entry's
        // remaining blocks (within this exclusive section), but the bytes go
        // nowhere. The channel holds no parser reference, so future #pending
        // state cannot be affected.
        await this.#drainBody(remaining);
        return;
      }
      const ok = channel.write(chunk);
      if (!ok) {
        await this.#drainBody(remaining);
        return;
      }
    }
    await channel.close();
  }

  #deliver(entry: TarEntry) {
    const waiter = this.#entryWaiters.shift();
    if (waiter) {
      // Consumer was already waiting: hand over immediately and free the
      // look-ahead slot the slot scanner reserved.
      waiter.resolve({ value: entry, done: false });
      this.#releaseSlot();
    } else {
      this.#entryQueue.push(entry);
    }
  }

  /**
   * Read the next physical header block, skipping end-of-archive zero padding
   * (which also walks straight into a concatenated archive) and
   * resynchronising across corrupt headers. Extension metadata bound to a
   * skipped header is explicitly discarded here.
   */
  async #nextHeaderBlock(): Promise<Uint8Array | null> {
    while (!this.#aborted) {
      const block = await this.#take(BLOCK);
      if (!block) return null;

      if (isZeroBlock(block)) {
        // Archive boundary / padding: any extension record still pending was
        // meant for an entry that never came. Drop it, then keep scanning in
        // case another archive follows.
        this.#pending = null;
        continue;
      }

      if (isPlausibleHeader(block)) return block;

      // Corrupt header: explicitly discard the extension record bound to it,
      // then resync block-by-block. #pending is not touched again until a
      // fresh longname/longlink record appears.
      this.#pending = null;
    }
    return null;
  }

  /**
   * Read one physical body block (always a full 512-byte tar block) and
   * return the usable prefix (the final block may be partly zero padding).
   */
  async #bodyBlock(remaining: number): Promise<Uint8Array> {
    const block = await this.#take(BLOCK);
    if (!block || block.length < BLOCK) {
      throw new Error('Unexpected end of TAR entry body');
    }
    return block.subarray(0, Math.min(remaining, BLOCK));
  }

  async #drainBody(size: number): Promise<void> {
    let remaining = size;
    while (remaining > 0) {
      if (this.#aborted) return;
      const chunk = await this.#bodyBlock(remaining);
      remaining -= chunk.length;
    }
  }

  async #readStringBody(size: number): Promise<string> {
    const parts: Uint8Array[] = [];
    let remaining = size;
    while (remaining > 0) {
      if (this.#aborted) throw new Error('TarReader cancelled');
      const chunk = await this.#bodyBlock(remaining);
      parts.push(chunk);
      remaining -= chunk.length;
    }
    return decoder.decode(concat(parts));
  }

  /**
   * Read exactly one 512-byte tar block from the source, tolerating arbitrary
   * source chunk boundaries via an internal buffer. Returns null at clean EOF
   * or a short block when the archive is truncated.
   *
   * Must be called inside a section guarded by #runExclusive (header scan) or
   * an entry lock handed off to #pumpBody: sections never interleave, so the
   * archive is always consumed in strict physical order.
   */
  async #take(length: number): Promise<Uint8Array | null> {
    while (this.#bufferedBytes < length) {
      const result = await this.#source.next();
      if (result.done) {
        if (this.#bufferedBytes === 0) return null;
        return this.#drainBuffer(Math.min(length, this.#bufferedBytes));
      }
      const chunk = result.value;
      if (chunk.length > 0) {
        this.#buffered.push(chunk);
        this.#bufferedBytes += chunk.length;
      }
    }
    return this.#drainBuffer(length);
  }

  /**
   * Run `work` as one exclusive source section, chained after any body pump
   * still draining the previous entry.
   */
  #runExclusive<T>(work: () => Promise<T>): Promise<T> {
    const run = this.#sectionGate.then(work, work);
    // Keep the chain alive regardless of the outcome of `work`; callers see
    // the real result via the returned promise.
    this.#sectionGate = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  #drainBuffer(length: number): Uint8Array {
    if (
      this.#buffered.length === 1 &&
      this.#buffered[0].length === length
    ) {
      const out = this.#buffered.pop()!;
      this.#bufferedBytes = 0;
      return out;
    }
    const out = new Uint8Array(length);
    let offset = 0;
    while (offset < length) {
      const head = this.#buffered[0];
      const need = length - offset;
      if (head.length <= need) {
        out.set(head, offset);
        offset += head.length;
        this.#buffered.shift();
      } else {
        out.set(head.subarray(0, need), offset);
        this.#buffered[0] = head.subarray(need);
        offset = length;
      }
    }
    this.#bufferedBytes -= length;
    return out;
  }
}

// ---------------------------------------------------------------------------
// Header parsing
// ---------------------------------------------------------------------------

type ParsedHeader =
  | {
      kind: 'entry';
      path: string;
      linkName: string;
      size: number;
      type: TarType;
    }
  | { kind: 'longname' | 'longlink'; size: number };

function parseHeader(block: Uint8Array): ParsedHeader {
  let name = readCString(block, 0, 100);
  const size = parseOctal(block, 124, 12);
  const typeflag = block[156] ?? 0;
  const linkName = readCString(block, 157, 100);

  const magic = readCString(block, 257, 6);
  if (magic === 'ustar') {
    const prefix = readCString(block, 345, 155);
    if (prefix.length > 0) {
      name = prefix.endsWith('/') ? prefix + name : `${prefix}/${name}`;
    }
  }

  switch (typeflag) {
    case 0x4c: // 'L' GNU longname
      return { kind: 'longname', size };
    case 0x4b: // 'K' GNU longlink
      return { kind: 'longlink', size };
    case 0x35: // '5' directory
      return { kind: 'entry', path: name, linkName, size, type: 'directory' };
    case 0x31: // '1' hard link
    case 0x32: // '2' symbolic link
      return { kind: 'entry', path: name, linkName, size, type: 'link' };
    default:
      // '0', NUL and other regular-file variants; unknown flags treated as file.
      return { kind: 'entry', path: name, linkName, size, type: 'file' };
  }
}

const KNOWN_TYPE_FLAGS = new Set([
  0x00, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x4c, 0x4b, 0x56, 0x45,
]);

function isPlausibleHeader(block: Uint8Array): boolean {
  if (!checksumValid(block)) return false;
  if (!KNOWN_TYPE_FLAGS.has(block[156])) return false;
  const size = parseOctalSafe(block, 124, 12);
  return size !== null && Number.isSafeInteger(size) && size >= 0;
}

function checksumValid(block: Uint8Array): boolean {
  const stored = parseOctalSafe(block, 148, 8);
  if (stored === null) return false;
  let sum = 0;
  for (let i = 0; i < BLOCK; i++) {
    sum += i >= 148 && i < 156 ? 0x20 : block[i];
  }
  return sum === stored;
}

function parseOctal(block: Uint8Array, offset: number, length: number): number {
  return parseOctalSafe(block, offset, length) ?? 0;
}

function parseOctalSafe(
  block: Uint8Array,
  offset: number,
  length: number,
): number | null {
  let value = 0;
  let sawDigit = false;
  for (let i = 0; i < length; i++) {
    const byte = block[offset + i];
    if (byte === 0 || byte === 0x20) {
      if (sawDigit) break;
      continue;
    }
    if (byte < 0x30 || byte > 0x37) return null;
    value = value * 8 + (byte - 0x30);
    sawDigit = true;
  }
  return sawDigit ? value : 0;
}

function readCString(
  block: Uint8Array,
  offset: number,
  length: number,
): string {
  let end = offset;
  const limit = offset + length;
  while (end < limit && block[end] !== 0) end++;
  return decoder.decode(block.subarray(offset, end));
}

function stripTrailingNuls(text: string): string {
  let end = text.length;
  while (end > 0 && text.charCodeAt(end - 1) === 0) end--;
  return text.slice(0, end);
}

function concat(parts: Uint8Array[]): Uint8Array {
  if (parts.length === 1) return parts[0];
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function isZeroBlock(block: Uint8Array): boolean {
  for (let i = 0; i < block.length; i++) {
    if (block[i] !== 0) return false;
  }
  return true;
}
