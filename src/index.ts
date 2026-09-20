/**
 * TAR 读取核心。
 *
 * 关键约定（修复 longname 串号与共享解析状态问题）：
 *
 * 1. GNU longname ('L') / longlink ('K') 记录只是“绑定到下一个物理头”的
 *    不可变待处理元数据，分别存放在两个独立槽位中；读到后续物理头的瞬间
 *    立即快照消费并清空槽位——无论该条目是否被 filter 跳过、也无论调用方
 *    是否读取正文。
 * 2. 物理头损坏时按块边界重同步，并明确丢弃已绑定的扩展记录
 *    （longname/longlink 不允许越过损坏头应用到再下一项）。
 * 3. 每个条目的正文在 yield 前已完整捕获为该条目私有的不可变快照；条目
 *    暴露的流每次基于快照新建，调用方延迟消费、并行消费或提前取消都不会
 *    回写解析器状态，也不会影响后续条目。
 */

const BLOCK = 512;

export type TarType = 'file' | 'directory' | 'link';

export type TarHeader = {
  path: string;
  size: number;
  type: TarType;
  /** 硬链接目标（GNU longlink 'K' 会覆盖头里的 linkname 字段）。 */
  linkName?: string;
};

export type TarEntry = TarHeader & {
  /** 条目在归档中的字节偏移（物理头起始位置）。 */
  offset: number;
  /**
   * 返回一个全新的、仅读取本条目正文快照的 Web ReadableStream。
   * 可多次调用、可并行消费；取消任意一个流都不影响解析器或其它流。
   */
  body: () => ReadableStream<Uint8Array>;
  /** 一次性返回正文副本。 */
  arrayBuffer: () => Promise<Uint8Array>;
  text: () => Promise<string>;
};

export type TarFilter = (header: TarHeader, offset: number) => boolean;

export type ParseTarOptions = {
  /**
   * 返回 false 的条目不会 yield 给调用方，但其物理头与正文照常消费，
   * 且绑定在其上的 longname/longlink 照常消耗（不会泄漏到后续条目）。
   */
  filter?: TarFilter;
};

/** 解析过程中发现的可恢复/致命问题，方便测试与诊断。 */
export class TarParseError extends Error {
  constructor(
    message: string,
    readonly offset: number,
  ) {
    super(`${message} (offset=${offset})`);
    this.name = 'TarParseError';
  }
}

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

/* ------------------------------------------------------------------ */
/* 字节源抽象                                                          */
/* ------------------------------------------------------------------ */

type ByteSource =
  | Uint8Array
  | Iterable<Uint8Array>
  | AsyncIterable<Uint8Array>
  | ReadableStream<Uint8Array>;

function toAsyncIterator(source: ByteSource): AsyncIterator<Uint8Array> {
  if (source instanceof Uint8Array) {
    let done = false;
    return {
      next: async () => {
        if (done) return { value: undefined, done: true };
        done = true;
        return { value: source, done: false };
      },
    };
  }
  if (typeof (source as AsyncIterable<Uint8Array>)[Symbol.asyncIterator] === 'function') {
    return (source as AsyncIterable<Uint8Array>)[Symbol.asyncIterator]();
  }
  const sync = (source as Iterable<Uint8Array>)[Symbol.iterator]();
  return {
    next: async () => sync.next() as IteratorResult<Uint8Array>,
  };
}

/** 跨 chunk 的精确字节读取队列。 */
class ByteQueue {
  #chunks: Uint8Array[] = [];
  #length = 0;
  #ended = false;

  constructor(private readonly input: AsyncIterator<Uint8Array>) {}

  /**
   * 读取恰好 size 字节。
   * - 干净 EOF（一个字节都没有）返回 `null`；
   * - 中途截断抛出 TarParseError。
   */
  async readExact(size: number, streamOffset: number): Promise<Uint8Array | null> {
    while (this.#length < size) {
      if (this.#ended) {
        if (this.#length === 0) return null;
        throw new TarParseError(
          'unexpected end of archive: truncated block',
          streamOffset + this.#length,
        );
      }
      const r = await this.input.next();
      if (r.done) this.#ended = true;
      else if (r.value && r.value.length) {
        this.#chunks.push(r.value);
        this.#length += r.value.length;
      }
    }

    const out = new Uint8Array(size);
    let filled = 0;
    while (filled < size) {
      const head = this.#chunks[0];
      const need = size - filled;
      if (head.length <= need) {
        out.set(head, filled);
        filled += head.length;
        this.#chunks.shift();
      } else {
        out.set(head.subarray(0, need), filled);
        this.#chunks[0] = head.subarray(need);
        filled = size;
      }
    }
    this.#length -= size;
    return out;
  }
}

/* ------------------------------------------------------------------ */
/* 头解析                                                              */
/* ------------------------------------------------------------------ */

function readString(block: Uint8Array, start: number, end: number): string {
  let n = end;
  while (n > start && block[n - 1] === 0) n--;
  return new TextDecoder().decode(block.subarray(start, n));
}

function readOctal(block: Uint8Array, start: number, end: number): number {
  let n = 0;
  for (let i = start; i < end; i++) {
    const c = block[i];
    if (c === 0 || c === 0x20) continue;
    // GNU 大尺寸二进制基-256 首字节：本实现不支持。
    if (c === 0x80) throw new TarParseError('base-256 size is not supported', start);
    if (c < 0x30 || c > 0x37) break;
    n = n * 8 + (c - 0x30);
  }
  return n;
}

function checksumValid(block: Uint8Array): boolean {
  let unsigned = 0;
  let signed = 0;
  for (let i = 0; i < BLOCK; i++) {
    if (i >= 148 && i < 156) {
      // 校验和字段本身按空格计入。
      unsigned += 0x20;
      signed += 0x20;
    } else {
      unsigned += block[i];
      signed += block[i] > 0x7f ? block[i] - 256 : block[i];
    }
  }
  const stored = readOctal(block, 148, 156);
  return stored === unsigned || stored === signed;
}

function isZeroBlock(block: Uint8Array): boolean {
  for (let i = 0; i < BLOCK; i++) if (block[i] !== 0) return false;
  return true;
}

/** 校验通过后解析一个普通条目头；boundName/boundLink 是被即时消费的扩展元数据。 */
function parseHeader(
  block: Uint8Array,
  boundName: string | null,
  boundLink: string | null,
): TarHeader {
  let path = readString(block, 0, 100);
  const prefix = readString(block, 345, 500);
  if (prefix) path = `${prefix}/${path}`;
  if (boundName !== null) path = boundName;

  const size = readOctal(block, 124, 136);

  let linkName = readString(block, 157, 257);
  if (boundLink !== null) linkName = boundLink;

  const flag = String.fromCharCode(block[156] || 0x30);
  let type: TarType;
  if (flag === '5') type = 'directory';
  else if (flag === '1' || flag === '2') type = 'link';
  else type = 'file';

  const header: TarHeader = { path, size, type };
  if (linkName) header.linkName = linkName;
  return header;
}

/* ------------------------------------------------------------------ */
/* 正文流                                                              */
/* ------------------------------------------------------------------ */

function makeBodyStream(snapshot: Uint8Array): ReadableStream<Uint8Array> {
  // 快照在条目创建时定型；流只持有这一个不可变引用，与解析器再无共享状态。
  let position = 0;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (position >= snapshot.length) {
        controller.close();
        return;
      }
      // 64KiB 分块，让大正文也能被分段观察/提前取消。
      const end = Math.min(position + 64 * 1024, snapshot.length);
      controller.enqueue(snapshot.subarray(position, end));
      position = end;
    },
  });
}

/* ------------------------------------------------------------------ */
/* 解析器                                                              */
/* ------------------------------------------------------------------ */

/**
 * 流式解析 TAR 归档。
 *
 * 对每个物理头：
 *  - 'L'（GNU longname）/ 'K'（GNU longlink）：读入不可变名称存入对应待处理
 *    槽位，等待绑定下一个物理头；
 *  - 其它有效头：立即取出并清空待处理槽位（消费与 filter / 调用方无关），
 *    读完整正文形成私有快照，再按 filter 决定是否 yield；
 *  - 校验失败：按块边界重同步，明确丢弃已绑定的扩展记录。
 */
export async function* parseTar(
  source: ByteSource,
  options: ParseTarOptions = {},
): AsyncGenerator<TarEntry> {
  const filter = options.filter ?? (() => true);
  const queue = new ByteQueue(toAsyncIterator(source));

  // 绑定“下一个物理头”的不可变待处理元数据。两个槽位互相独立，
  // 连续的 L/K 记录分别累积；一旦下一个物理头被读取即全部清空。
  let pendingName: string | null = null;
  let pendingLink: string | null = null;

  // 上一块是否为零块：单零块后再遇有效头，即串联归档的下一卷开头。
  let sawZero = false;
  let offset = 0;

  /** 读取 n 字节（n 不超过 512，仅用于块/块内余量），并推进 offset。 */
  const readBlock = async (n: number): Promise<Uint8Array | null> => {
    const data = await queue.readExact(n, offset);
    if (data !== null) offset += data.length;
    return data;
  };

  /** 读取一条记录的正文（size 字节）并跳过对齐填充。 */
  const readPayload = async (size: number, label: string): Promise<Uint8Array | null> => {
    const payload = new Uint8Array(size);
    let filled = 0;
    while (filled < size) {
      const part = await readBlock(Math.min(BLOCK, size - filled));
      if (part === null) throw new TarParseError(`unexpected end of archive in ${label}`, offset);
      payload.set(part, filled);
      filled += part.length;
    }
    const padding = (BLOCK - (size % BLOCK)) % BLOCK;
    if (padding) {
      const pad = await readBlock(padding);
      if (pad === null) throw new TarParseError(`unexpected end of archive in ${label} padding`, offset);
    }
    return payload;
  };

  // 主循环每次处理“一个物理头块”。重同步找到的合法块通过
  // pendingBlock 直接进入处理流程，避免重复读块。
  let block = await readBlock(BLOCK);

  while (block !== null) {
    const headerOffset = offset - BLOCK;

    if (isZeroBlock(block)) {
      sawZero = true;
      // 卷结束：未被任何物理头消费的扩展记录随本卷一起丢弃，
      // 绝不允许绑定到串联归档的下一卷。
      pendingName = null;
      pendingLink = null;
      block = await readBlock(BLOCK);
      continue;
    }

    if (!checksumValid(block)) {
      if (sawZero) {
        // 零块之后只可能是 EOF 或下一卷有效头；垃圾数据一律致命，
        // 避免把填充误判成新卷。
        throw new TarParseError('invalid block after end-of-archive marker', headerOffset);
      }
      // 损坏头恢复：明确丢弃已经绑定的扩展记录，然后按块边界重同步。
      pendingName = null;
      pendingLink = null;
      block = await readBlock(BLOCK);
      while (block !== null && !checksumValid(block)) {
        block = await readBlock(BLOCK);
      }
      continue;
    }

    sawZero = false;
    const flag = String.fromCharCode(block[156] || 0x30);
    const size = readOctal(block, 124, 136);

    if (flag === 'L' || flag === 'K') {
      // GNU 扩展记录：读入正文并解码为不可变待处理元数据。
      const payload = await readPayload(size, 'long name record');
      if (payload === null) throw new TarParseError('unexpected end of archive in long name record', offset);
      let name = new TextDecoder().decode(payload);
      const nul = name.indexOf('\0');
      if (nul !== -1) name = name.slice(0, nul);

      if (flag === 'L') pendingName = name;
      else pendingLink = name;

      block = await readBlock(BLOCK);
      continue;
    }

    // 普通条目：在读取该物理头的瞬间立即消费并清空待处理槽位。
    // 快照进入局部常量，随后无论 filter 跳过、yield 竞态或正文流如何
    // 使用，都无法回改解析器状态。
    const boundName = pendingName;
    const boundLink = pendingLink;
    pendingName = null;
    pendingLink = null;

    const header = parseHeader(block, boundName, boundLink);

    // 完整捕获正文为条目私有快照（对齐填充一并跳过）。
    const snapshot = new Uint8Array(size);
    let filled = 0;
    while (filled < size) {
      const part = await readBlock(Math.min(BLOCK, size - filled));
      if (part === null) throw new TarParseError('unexpected end of archive in entry body', offset);
      snapshot.set(part, filled);
      filled += part.length;
    }
    const padding = (BLOCK - (size % BLOCK)) % BLOCK;
    if (padding) {
      const pad = await readBlock(padding);
      if (pad === null) throw new TarParseError('unexpected end of archive in body padding', offset);
    }

    if (filter(header, headerOffset)) {
      const bodySnapshot = snapshot;
      const entry: TarEntry = {
        ...header,
        offset: headerOffset,
        body: () => makeBodyStream(bodySnapshot),
        arrayBuffer: async () => {
          const copy = new Uint8Array(bodySnapshot.length);
          copy.set(bodySnapshot);
          return copy;
        },
        text: async () => new TextDecoder().decode(bodySnapshot),
      };
      // 交给调用方；正文快照独立存活，解析器继续推进不受影响。
      yield entry;
    }

    block = await readBlock(BLOCK);
  }
}
