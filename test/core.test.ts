import {describe, it} from 'node:test';
import assert from 'node:assert/strict';
import {parseTar, TarParseError, type TarEntry, type TarFilter} from '../src/index.js';

/* ---------------- TAR 构造辅助（仅用于测试） ---------------- */

const BLOCK = 512;
const encoder = new TextEncoder();

function pad(n: number): number {
  return (BLOCK - (n % BLOCK)) % BLOCK;
}

function octal(value: number, length: number): Uint8Array {
  const s = value.toString(8);
  const out = new Uint8Array(length);
  out.fill(0);
  const body = s.padStart(length - 1, '0');
  out.set(encoder.encode(body), 0);
  return out;
}

/** 生成一个 ustar 头（可调字段），自动计算校验和。 */
function header(fields: {
  name: string;
  size?: number;
  type?: string;
  linkName?: string;
  prefix?: string;
}): Uint8Array {
  const block = new Uint8Array(BLOCK);
  const dv = new DataView(block.buffer);
  const setStr = (off: number, max: number, s: string) => {
    const bytes = encoder.encode(s).subarray(0, max);
    block.set(bytes, off);
  };
  setStr(0, 100, fields.name);
  block.set(octal(0o644, 8), 100); // mode
  block.set(octal(0, 8), 108); // uid
  block.set(octal(0, 8), 116); // gid
  block.set(octal(fields.size ?? 0, 12), 124); // size
  block.set(octal(0, 12), 136); // mtime
  // 148..155 checksum: 先放空格
  for (let i = 148; i < 156; i++) block[i] = 0x20;
  block[156] = (fields.type ?? '0').charCodeAt(0); // typeflag
  setStr(157, 100, fields.linkName ?? '');
  setStr(257, 6, 'ustar'); // magic（无 NUL 尾，兼容读取）
  block[263] = 0;
  block[264] = 0;
  if (fields.prefix) setStr(345, 155, fields.prefix);

  let sum = 0;
  for (let i = 0; i < BLOCK; i++) sum += block[i];
  dv.setUint8(148, 0x30);
  const cs = sum.toString(8).padStart(6, '0') + '\0 ';
  block.set(encoder.encode(cs), 148);
  return block;
}

/** 普通文件条目：头 + 正文 + 对齐。 */
function fileEntry(name: string, content: string | Uint8Array, type = '0', linkName?: string): Uint8Array {
  const body = typeof content === 'string' ? encoder.encode(content) : content;
  const parts = [header({name, size: body.length, type, linkName}), body, new Uint8Array(pad(body.length))];
  return concat(parts);
}

/** GNU longname/longlink 记录。 */
function gnuRecord(flag: 'L' | 'K', name: string): Uint8Array {
  const payload = encoder.encode(name + '\0');
  return concat([
    header({name: (flag === 'L' ? '././@LongLink' : '././@LongLink'), size: payload.length, type: flag}),
    payload,
    new Uint8Array(pad(payload.length)),
  ]);
}

function concat(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

const ZERO = new Uint8Array(BLOCK);
const TWO_ZEROS = concat([ZERO, ZERO]);

/** 把归档切成随机大小的 chunk，模拟流式/任意调度。 */
function chunkify(data: Uint8Array, size: number): AsyncIterable<Uint8Array> {
  return {
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < data.length; i += size) {
        yield data.subarray(i, i + size);
      }
    },
  };
}

async function names(source: Parameters<typeof parseTar>[0], filter?: TarFilter): Promise<string[]> {
  const out: string[] = [];
  for await (const e of parseTar(source, filter ? {filter} : {})) out.push(e.path);
  return out;
}

async function drain(stream: ReadableStream<Uint8Array>): Promise<string> {
  const chunks: Uint8Array[] = [];
      for await (const c of stream as unknown as AsyncIterable<Uint8Array>) chunks.push(c);
  return new TextDecoder().decode(concat(chunks));
}

/* ------------------------------ 测试 ------------------------------ */

describe('GNU longname/longlink 绑定语义', () => {
  it('longname 应用到紧邻的下一个物理头', async () => {
    const arc = concat([
      gnuRecord('L', 'a/very/long/file/name.txt'),
      fileEntry('short.txt', 'hello'),
      fileEntry('plain.txt', 'world'),
      TWO_ZEROS,
    ]);
    assert.deepEqual(await names(arc), ['a/very/long/file/name.txt', 'plain.txt']);
  });

  it('被 filter 跳过的头仍会消费 longname，不泄漏到再下一项', async () => {
    const arc = concat([
      gnuRecord('L', 'a/very/long/file/name.txt'),
      fileEntry('short.txt', 'hello'), // 此项被跳过
      fileEntry('plain.txt', 'world'),
      TWO_ZEROS,
    ]);
    const got = await names(arc, (h) => h.path !== 'a/very/long/file/name.txt');
    assert.deepEqual(got, ['plain.txt']);
  });

  it('longlink 独立于 longname，且两者同时绑定时各归其位', async () => {
    const arc = concat([
      gnuRecord('K', 'a/very/long/link/target'),
      gnuRecord('L', 'a/very/long/link/name'),
      fileEntry('lnk', '', '1', 'short-target'),
      fileEntry('next', 'x'),
      TWO_ZEROS,
    ]);
    const entries: TarEntry[] = [];
    for await (const e of parseTar(arc)) entries.push(e);
    assert.equal(entries[0].path, 'a/very/long/link/name');
    assert.equal(entries[0].linkName, 'a/very/long/link/target');
    assert.equal(entries[1].path, 'next');
    assert.equal(entries[1].linkName, undefined);
  });

  it('连续两条 longname：后者覆盖前者，且只作用一次', async () => {
    const arc = concat([
      gnuRecord('L', 'first/long/name'),
      gnuRecord('L', 'second/long/name'),
      fileEntry('a', '1'),
      fileEntry('b', '2'),
      TWO_ZEROS,
    ]);
    assert.deepEqual(await names(arc), ['second/long/name', 'b']);
  });

  it('空名称 longname：覆盖为空前不会回退到短名，后续条目不受影响', async () => {
    const arc = concat([
      gnuRecord('L', ''),
      fileEntry('short.txt', 'hello'),
      fileEntry('plain.txt', 'world'),
      TWO_ZEROS,
    ]);
    assert.deepEqual(await names(arc), ['', 'plain.txt']);
  });
});

describe('损坏头恢复', () => {
  it('紧随 longname 的头损坏时，longname 被丢弃，再下一项使用自身短名', async () => {
    const good = fileEntry('plain.txt', 'world');
    const corrupt = new Uint8Array(BLOCK);
    // 随机垃圾（校验和几乎必错）
    for (let i = 0; i < BLOCK; i++) corrupt[i] = (i * 37 + 11) & 0xff;
    const arc = concat([
      gnuRecord('L', 'a/very/long/file/name.txt'),
      corrupt, // 被绑定的物理头损坏
      good,
      TWO_ZEROS,
    ]);
    assert.deepEqual(await names(arc), ['plain.txt']);
  });

  it('损坏头恢复：可跳过多个坏块并重同步到合法头，扩展记录随之丢弃', async () => {
    const junkA = new Uint8Array(BLOCK);
    const junkB = new Uint8Array(BLOCK);
    for (let i = 0; i < BLOCK; i++) {
      junkA[i] = (i * 7 + 3) & 0xff;
      junkB[i] = (i * 13 + 5) & 0xff;
    }
    const arc = concat([
      gnuRecord('K', 'dangling/link'),
      junkA,
      junkB,
      fileEntry('recovered', 'ok'),
      TWO_ZEROS,
    ]);
    const entries: TarEntry[] = [];
    for await (const e of parseTar(arc)) entries.push(e);
    assert.equal(entries.length, 1);
    assert.equal(entries[0].path, 'recovered');
    assert.equal(entries[0].linkName, undefined);
  });

  it('正文截断时抛出带偏移的 TarParseError', async () => {
    const arc = concat([
      header({name: 'big', size: 1000}),
      encoder.encode('only-a-few-bytes'),
    ]);
    await assert.rejects(() => parseTar(arc).next(), TarParseError);
  });
});

describe('正文快照与读取调度', () => {
  it('正文延迟消费：先枚举完所有条目再读流，内容仍然正确', async () => {
    const arc = concat([
      fileEntry('a', 'AAA'),
      fileEntry('b', 'BBB'),
      fileEntry('c', 'CCC'),
      TWO_ZEROS,
    ]);
    const entries: TarEntry[] = [];
    for await (const e of parseTar(arc)) entries.push(e);
    assert.deepEqual(
      await Promise.all(entries.map((e) => e.text())),
      ['AAA', 'BBB', 'CCC'],
    );
  });

  it('多个正文流可并行、重复消费且互不干扰', async () => {
    const arc = concat([fileEntry('a', 'x'.repeat(200_000)), TWO_ZEROS]);
    let entry!: TarEntry;
    for await (const e of parseTar(arc)) entry = e;
    const s1 = entry.body();
    const s2 = entry.body();
    const [a, b, c] = await Promise.all([drain(s1), drain(s2), entry.text()]);
    assert.equal(a.length, 200_000);
    assert.equal(a, b);
    assert.equal(a, c);
  });

  it('调用方提前关闭/取消正文流，不影响解析器与后续条目', async () => {
    const arc = concat([
      fileEntry('a', 'a'.repeat(10_000)),
      fileEntry('b', 'B'.repeat(10_000)),
      fileEntry('c', 'c'.repeat(10_000)),
      TWO_ZEROS,
    ]);
    const entries: TarEntry[] = [];
    for await (const e of parseTar(arc)) {
      const stream = e.body();
      const reader = stream.getReader();
      await reader.read();
      await reader.cancel(new Error('caller lost interest'));
      entries.push(e);
    }
    assert.deepEqual(entries.map((e) => e.path), ['a', 'b', 'c']);
    assert.equal(await entries[2].text(), 'c'.repeat(10_000));
  });

  it('调用方提前 break 迭代器后，已取得条目的快照流仍可读', async () => {
    const arc = concat([
      fileEntry('a', 'aaa'),
      fileEntry('b', 'bbb'),
      TWO_ZEROS,
    ]);
    let first!: TarEntry;
    for await (const e of parseTar(arc)) {
      first = e;
      break;
    }
    assert.equal(await first.text(), 'aaa');
  });
});

describe('串联归档', () => {
  it('两个归档直接串联：零块不产出条目，名称序列为两卷之和', async () => {
    const vol1 = concat([fileEntry('one', '1'), ZERO]);
    const vol2 = concat([fileEntry('two', '2'), TWO_ZEROS]);
    assert.deepEqual(await names(concat([vol1, vol2])), ['one', 'two']);
  });

  it('卷尾悬挂的 longname 不会绑定到下一卷的第一个条目', async () => {
    // 第一卷：longname 后直接是零块（无实体头），扩展记录必须随卷丢弃。
    const vol1 = concat([gnuRecord('L', 'dangling/from/volume-one'), ZERO]);
    const vol2 = concat([fileEntry('two', '2'), TWO_ZEROS]);
    assert.deepEqual(await names(concat([vol1, vol2])), ['two']);
  });
});

describe('读取调度无关性', () => {
  const arc = concat([
    gnuRecord('L', 'some/quite/long/name.dat'),
    fileEntry('x', 'X'.repeat(5_000)),
    fileEntry('y', 'YY'),
    gnuRecord('K', 'link/target/very/long'),
    fileEntry('z', '', '1', 't'),
    ZERO,
    fileEntry('p', 'P'),
    TWO_ZEROS,
  ]);
  const expected = ['some/quite/long/name.dat', 'y', 'z', 'p'];
  const expectedLinks: (string | undefined)[] = [undefined, undefined, 'link/target/very/long', undefined];

  const schedules = {
    /** 整块读取，不消费正文 */
    wholeBlocksNoBody: 512,
    /** 1 字节流（极端跨块） */
    byteByByte: 1,
    /** 与块不对齐 */
    odd333: 333,
    /** 大块，天然跨多个头 */
    huge: 5000,
    /** 质数大小 */
    prime101: 101,
  };

  for (const [label, chunkSize] of Object.entries(schedules)) {
    it(`分块 ${label}（${chunkSize}B/块）得到相同名称序列`, async () => {
      const paths: string[] = [];
      const links: (string | undefined)[] = [];
      for await (const e of parseTar(chunkify(arc, chunkSize))) {
        paths.push(e.path);
        links.push(e.linkName);
      }
      assert.deepEqual(paths, expected);
      assert.deepEqual(links, expectedLinks);
    });
  }

  it('不同分块 + 全部正文即时消费 / 延迟消费，名称序列一致', async () => {
    const run = async (chunkSize: number, lazy: boolean): Promise<string[]> => {
      const collected: TarEntry[] = [];
      for await (const e of parseTar(chunkify(arc, chunkSize))) {
        if (lazy) collected.push(e);
        else {
          await e.text();
          collected.push(e);
        }
      }
      if (lazy) await Promise.all(collected.map((e) => e.text()));
      return collected.map((e) => e.path);
    };
    for (const size of [1, 101, 333, 512, 5000]) {
      const eager = await run(size, false);
      const lazy = await run(size, true);
      assert.deepEqual(eager, expected, `eager @ ${size}`);
      assert.deepEqual(lazy, expected, `lazy @ ${size}`);
    }
  });
});
