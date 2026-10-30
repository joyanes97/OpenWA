import { once } from 'events';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { PassThrough, Writable } from 'stream';
import { setTimeout as realSleep } from 'timers/promises';
import { ConfigService } from '@nestjs/config';

// The SDK is mocked: each test hands GetObject a body it drives itself, and ListObjectsV2 a page that never
// answers. Fake timers drive the bounds; what the streams do on their own is awaited in real time.
const mockSend = jest.fn();
jest.mock('@aws-sdk/client-s3', () => {
  const S3Client = jest.fn().mockImplementation(() => ({ send: mockSend }));
  return {
    S3Client,
    HeadBucketCommand: jest.fn(),
    CreateBucketCommand: jest.fn(),
    ListObjectsV2Command: jest.fn(),
    GetObjectCommand: jest.fn(),
    PutObjectCommand: jest.fn(),
    DeleteObjectCommand: jest.fn(),
  };
});

import { GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { S3_SOCKET_TIMEOUT_MS, StorageService } from './storage.service';
import { sweepOrphanedFiles } from './orphan-sweep';

const ENV_KEYS = ['S3_ENDPOINT', 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY', 'S3_BUCKET', 'S3_KEY_PREFIX'];

// Room past the real-time deadlines below, so a missed condition fails its expectation rather than the test.
jest.setTimeout(20_000);

/** Wait in real time for a condition the streams reach on their own, up to a deadline. */
async function until(condition: () => boolean, deadlineMs = 5_000): Promise<boolean> {
  for (let waited = 0; !condition() && waited < deadlineMs; waited += 10) await realSleep(10);
  return condition();
}

/** The work's outcome, or 'pending' once the deadline passes in real time. */
function within<T>(work: Promise<T>, deadlineMs = 5_000): Promise<T | 'pending'> {
  return Promise.race([work, realSleep(deadlineMs, 'pending' as const, { ref: false })]);
}

function s3Error(name: string): Error {
  return Object.assign(new Error(name), { name });
}

describe('StorageService S3 bounds after the response headers', () => {
  let tmpRoot: string;
  let localPath: string;
  let bodies: PassThrough[];

  beforeEach(() => {
    jest.useFakeTimers();
    tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'owa-s3-idle-bound-'));
    localPath = path.join(tmpRoot, 'media');
    for (const key of ENV_KEYS) delete process.env[key];
    bodies = [];
    mockSend.mockReset();
    mockSend.mockImplementation((cmd: unknown) => {
      if (!(cmd instanceof GetObjectCommand)) return Promise.resolve({});
      const body = new PassThrough();
      bodies.push(body);
      return Promise.resolve({ Body: body, ContentLength: 1024 * 1024 });
    });
  });

  afterEach(() => {
    for (const body of bodies) body.destroy();
    jest.useRealTimers();
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  });

  async function s3Storage(): Promise<StorageService> {
    const svc = new StorageService({
      get: (key: string) => {
        if (key === 'storage.type') return 's3';
        if (key === 'storage.localPath') return localPath;
        if (key === 'storage.s3') return { accessKeyId: 'k', secretAccessKey: 's', region: 'us-east-1' };
        return undefined;
      },
    } as unknown as ConfigService);
    jest.spyOn((svc as unknown as { logger: { warn: () => void } }).logger, 'warn').mockImplementation(() => undefined);
    expect(await until(() => (svc as unknown as { s3Available: boolean }).s3Available)).toBe(true);
    return svc;
  }

  const openDownloads = (svc: StorageService): number =>
    (svc as unknown as { s3OpenDownloads: number }).s3OpenDownloads;

  it('fails a download whose body sends nothing for the idle bound, and frees its place', async () => {
    const svc = await s3Storage();
    const { stream } = await svc.openFile('statuses/s1/stalled.mp4');
    const failed = once(stream, 'error');

    await jest.advanceTimersByTimeAsync(S3_SOCKET_TIMEOUT_MS - 1);
    expect(stream.destroyed).toBe(false);
    expect(openDownloads(svc)).toBe(1);

    await jest.advanceTimersByTimeAsync(1);
    const outcome = await within(failed);
    expect(outcome === 'pending' ? outcome : (outcome[0] as Error).name).toBe('TimeoutError');
    // The body holds the socket, so it is torn down with the stream and gives its place back.
    expect(await until(() => bodies[0].destroyed && openDownloads(svc) === 0)).toBe(true);
  });

  it('fails a whole-file read whose body sends nothing for the idle bound', async () => {
    const svc = await s3Storage();
    const timers = jest.getTimerCount();
    const read = svc.getFile('chat-media/s1/stalled.bin').then(
      () => 'resolved',
      (error: Error) => error.name,
    );
    // The bound starts once the store has answered with its headers.
    expect(await until(() => jest.getTimerCount() > timers)).toBe(true);

    await jest.advanceTimersByTimeAsync(S3_SOCKET_TIMEOUT_MS);
    expect(await within(read)).toBe('TimeoutError');
  });

  it('never cuts a download whose body keeps sending', async () => {
    const svc = await s3Storage();
    const { stream } = await svc.openFile('statuses/s1/steady.mp4');
    const received: Buffer[] = [];
    const errors: Error[] = [];
    stream.on('data', (chunk: Buffer) => received.push(chunk));
    stream.on('error', (error: Error) => errors.push(error));
    const ended = once(stream, 'end');

    // Four chunks, each just inside the bound: twice the bound passes in all, and nothing is cut.
    for (let i = 0; i < 4; i++) {
      await jest.advanceTimersByTimeAsync(S3_SOCKET_TIMEOUT_MS - 1);
      bodies[0].write(Buffer.alloc(1024, i));
      expect(await until(() => received.length === i + 1)).toBe(true);
    }
    bodies[0].end();

    expect(await within(ended)).not.toBe('pending');
    expect(errors).toEqual([]);
    expect(Buffer.concat(received)).toHaveLength(4 * 1024);
    expect(await until(() => openDownloads(svc) === 0)).toBe(true);
  });

  it('frees the place of a small object whose reader stopped once the store has sent all of it', async () => {
    const svc = await s3Storage();
    const { stream } = await svc.openFile('statuses/s1/small.jpg');
    const errors: Error[] = [];
    stream.on('error', (error: Error) => errors.push(error));

    // Nothing reads the stream, but the whole object is in, so the socket is back and so is the place.
    bodies[0].end(Buffer.alloc(8 * 1024, 7));
    expect(await until(() => openDownloads(svc) === 0 && (stream as unknown as Writable).writableFinished)).toBe(true);

    // The bytes still wait for the reader, past the bound.
    await jest.advanceTimersByTimeAsync(S3_SOCKET_TIMEOUT_MS * 2);
    const chunks: Buffer[] = [];
    for await (const chunk of stream) chunks.push(chunk as Buffer);
    expect(Buffer.concat(chunks)).toEqual(Buffer.alloc(8 * 1024, 7));
    expect(errors).toEqual([]);
    expect(openDownloads(svc)).toBe(0);
  });

  it('releases the place once for a download read to the end and for one whose reader went away', async () => {
    const svc = await s3Storage();

    const full = await svc.openFile('statuses/s1/full.jpg');
    bodies[0].end(Buffer.from('abc'));
    const chunks: Buffer[] = [];
    for await (const chunk of full.stream) chunks.push(chunk as Buffer);
    expect(Buffer.concat(chunks).toString()).toBe('abc');
    expect(await until(() => openDownloads(svc) === 0)).toBe(true);
    full.stream.destroy();

    const gone = await svc.openFile('statuses/s1/gone.jpg');
    expect(openDownloads(svc)).toBe(1);
    gone.stream.destroy();
    // The body behind it is torn down too: it holds the socket.
    expect(await until(() => bodies[1].destroyed && openDownloads(svc) === 0)).toBe(true);
    gone.stream.destroy();
    expect(openDownloads(svc)).toBe(0);
  });

  it('fails an export whose entry sends nothing for the idle bound', async () => {
    mockSend.mockImplementation((cmd: unknown) => {
      if (cmd instanceof ListObjectsV2Command) return Promise.resolve({ Contents: [{ Key: 'media/stalled.mp4' }] });
      if (!(cmd instanceof GetObjectCommand)) return Promise.resolve({});
      const body = new PassThrough();
      bodies.push(body);
      return Promise.resolve({ Body: body, ContentLength: 1024 * 1024 });
    });
    const svc = await s3Storage();
    jest
      .spyOn((svc as unknown as { logger: { error: () => void } }).logger, 'error')
      .mockImplementation(() => undefined);
    const timers = jest.getTimerCount();
    const output = await svc.createExportStream();
    output.resume();
    const failed = once(output, 'error').then(([error]: Error[]) => error.name);
    // The entry is open: the store has answered its GetObject with the headers.
    expect(await until(() => jest.getTimerCount() > timers)).toBe(true);

    await jest.advanceTimersByTimeAsync(S3_SOCKET_TIMEOUT_MS);
    expect(await within(failed)).toBe('TimeoutError');
  });

  describe('a ListObjectsV2 page that never answers', () => {
    let listed: boolean;
    let timeout: jest.SpyInstance;

    beforeEach(() => {
      listed = false;
      mockSend.mockImplementation((cmd: unknown, options?: { abortSignal?: AbortSignal }) => {
        if (!(cmd instanceof ListObjectsV2Command)) return Promise.resolve({});
        listed = true;
        return new Promise((_resolve, reject) => {
          options?.abortSignal?.addEventListener('abort', () => reject(s3Error('AbortError')));
        });
      });
      // AbortSignal.timeout runs on Node's internal timers, which fake timers do not reach.
      timeout = jest.spyOn(AbortSignal, 'timeout').mockImplementation((ms: number) => {
        const controller = new AbortController();
        setTimeout(() => controller.abort(), ms);
        return controller.signal;
      });
    });

    afterEach(() => timeout.mockRestore());

    async function rejectsOnceItsTimeoutPasses(list: (svc: StorageService) => Promise<unknown>): Promise<void> {
      const svc = await s3Storage();
      timeout.mockClear();
      const outcome = list(svc).then(
        () => 'resolved',
        (error: Error) => error.name,
      );
      expect(await until(() => listed)).toBe(true);
      // The page request carries its own bound.
      expect(timeout).toHaveBeenCalledTimes(1);
      const [ms] = timeout.mock.calls[0] as [number];

      await jest.advanceTimersByTimeAsync(ms - 1);
      await expect(Promise.race([outcome, Promise.resolve('pending')])).resolves.toBe('pending');
      await jest.advanceTimersByTimeAsync(1);
      expect(await within(outcome)).toBe('AbortError');
    }

    it('rejects the listing', async () => {
      await rejectsOnceItsTimeoutPasses(async svc => {
        for await (const file of svc.iterateFiles()) void file;
      });
    });

    it('rejects the file count', async () => {
      await rejectsOnceItsTimeoutPasses(svc => svc.getFileCount());
    });

    it('rejects an orphan sweep, which deletes nothing, not even an orphan listed before the page', async () => {
      fs.mkdirSync(path.join(localPath, 'statuses'), { recursive: true });
      fs.writeFileSync(path.join(localPath, 'statuses', 'orphan.jpg'), 'x');
      const deleteFile = jest.fn().mockResolvedValue(undefined);

      await rejectsOnceItsTimeoutPasses(svc =>
        sweepOrphanedFiles({
          storage: { iterateFiles: (prefix, options) => svc.iterateFiles(prefix, options), deleteFile },
          prefix: 'statuses/',
          graceMs: 0,
          now: 1000,
          firstSeenAt: new Map(),
          referencedAmong: () => Promise.resolve(new Set<string>()),
          onDeleteFailed: jest.fn(),
        }),
      );
      expect(deleteFile).not.toHaveBeenCalled();
    });
  });
});
