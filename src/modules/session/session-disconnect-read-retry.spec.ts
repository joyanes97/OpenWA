import { SessionEngineLifecycle } from './session-engine-lifecycle.service';
import { EngineRegistry } from '../../engine/engine-registry.service';
import { Session, SessionStatus } from './entities/session.entity';
import { AuditAction } from '../audit/entities/audit-log.entity';
import type { EngineEventCallbacks } from '../../engine/interfaces/whatsapp-engine.interface';

const ID = '66666666-6666-4666-8666-666666666666';
const BOUND = '628111';
const OTHER = '628999';
const WATCHDOG = 'liveness probe failed (watchdog)';

/** Yields to the event loop until `condition` holds, failing after a real-time deadline. */
async function until(condition: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${what}`);
    await new Promise(resolve => setImmediate(resolve));
  }
}

/**
 * A disconnect whose session read fails is handled again once the row can be read: the retry sits on the
 * episode's reconnect timer. The fixture drives the real lifecycle and registry; the repository's rejected
 * reads are the database failure under test (SQLITE_BUSY, a PostgreSQL failover).
 */
function fixture() {
  const stored = {
    id: ID,
    name: 'disconnect-read-retry',
    phone: BOUND,
    config: { reconnectBaseDelay: 1000 },
    status: SessionStatus.READY,
    desiredState: null,
    nodeId: null,
  } as unknown as Session;
  const repository = {
    findOne: jest.fn(() => Promise.resolve({ ...stored })),
    exists: jest.fn().mockResolvedValue(true),
    update: jest.fn((_where: unknown, patch: Partial<Session>) => {
      Object.assign(stored, patch);
      return Promise.resolve({ affected: 1 });
    }),
  };
  const engines: Array<{ initialize: jest.Mock }> = [];
  const factory = {
    create: jest.fn(() => {
      const engine = {
        initialize: jest.fn().mockResolvedValue(undefined),
        destroy: jest.fn().mockResolvedValue(undefined),
        forceDestroy: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        logout: jest.fn().mockResolvedValue(undefined),
      };
      engines.push(engine);
      return engine;
    }),
    purgeSessionData: jest.fn().mockResolvedValue(undefined),
  };
  const errors = new Map<string, string>();
  const state = {
    attachTo: <T>(value: T): T => value,
    clear: jest.fn((id: string) => errors.delete(id)),
    set: jest.fn((id: string, reason: string) => errors.set(id, reason)),
    get: jest.fn((id: string) => errors.get(id)),
  };
  const restrictions = { attachTo: <T>(value: T): T => value, clear: jest.fn(), clearIfDisprovedByReady: jest.fn() };
  const presence = { clear: jest.fn(), getOwnIntent: jest.fn() };
  const events = {
    emitSessionStatus: jest.fn(),
    emitSessionDisconnected: jest.fn(),
    emitSessionAuthenticated: jest.fn(),
    emitQRCode: jest.fn(),
  };
  const audit = { logWarn: jest.fn().mockResolvedValue(undefined), logInfo: jest.fn().mockResolvedValue(undefined) };
  const registry = new EngineRegistry();
  const lifecycle = new SessionEngineLifecycle(
    repository as never,
    {} as never,
    factory as never,
    registry,
    { clear: jest.fn() } as never,
    {} as never,
    state as never,
    restrictions as never,
    presence as never,
    events as never,
    { dispatch: jest.fn().mockResolvedValue(undefined) } as never,
    { execute: jest.fn().mockResolvedValue({ continue: true }) } as never,
    {} as never,
    undefined,
    undefined,
    audit as never,
  );
  // Every disconnect handling, the engine's, the watchdog's and a retry's, goes through this method.
  const handled = jest.spyOn(lifecycle, 'handleEngineDisconnected');
  const settled = async (): Promise<void> => {
    await Promise.all(handled.mock.results.map(result => result.value as Promise<void>));
  };
  const internals = lifecycle as unknown as {
    reconnectStates: Map<string, { timer: NodeJS.Timeout | null; initInFlight?: boolean; session?: Session }>;
    ownership?: { owns: () => boolean };
  };
  const reconnectState = () => internals.reconnectStates.get(ID);
  /** A lease lost: this node no longer owns the session (no ownership service means it owns them all). */
  const disown = (): void => {
    internals.ownership = { owns: () => false };
  };
  const unlinkAudits = (): unknown[][] =>
    (audit.logWarn.mock.calls as unknown[][]).filter(([action]) => action === AuditAction.SESSION_DISCONNECTED);
  const callbacksOf = (index: number): EngineEventCallbacks =>
    (engines[index].initialize.mock.calls as [EngineEventCallbacks][])[0][0];
  /** Fires the armed reconnect and waits until the engine it registers has finished initializing. */
  const runReconnect = async (count: number): Promise<void> => {
    await jest.advanceTimersByTimeAsync(3_000);
    await until(
      () => engines.length === count && engines[count - 1].initialize.mock.calls.length > 0,
      `engine ${count} to initialize`,
    );
    await until(() => !reconnectState()?.initInFlight, `reconnect ${count} to settle`);
  };
  return {
    lifecycle,
    registry,
    repository,
    stored,
    engines,
    audit,
    callbacksOf,
    settled,
    reconnectState,
    runReconnect,
    disown,
    unlinkAudits,
  };
}

describe('a disconnect read retry armed for an engine already replaced', () => {
  afterEach(() => jest.useRealTimers());

  it.each([
    ['fails', true],
    ['answers', false],
  ])('leaves the next engine drop to be handled when the replaced engine read %s', async (_outcome, readFails) => {
    const f = fixture();
    await f.lifecycle.start(ID);
    const first = f.callbacksOf(0);
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask', 'Date'] });

    // The watchdog arms a reconnect; the engine then reports a drop whose read is slow.
    await f.lifecycle.handleEngineDisconnected(ID, f.registry.get(ID)!, WATCHDOG);
    let answer!: (row: Session) => void;
    let fail!: (error: Error) => void;
    f.repository.findOne.mockImplementationOnce(
      () =>
        new Promise<Session>((resolve, reject) => {
          answer = resolve;
          fail = reject;
        }),
    );
    first.onDisconnected?.('NAVIGATION');

    // The reconnect fires and replaces the engine before that read settles.
    await f.runReconnect(2);
    if (readFails) fail(new Error('SQLITE_BUSY'));
    else answer({ ...f.stored });
    await f.settled();

    // The new engine drops, and its own read fails too.
    f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY'));
    f.callbacksOf(1).onDisconnected?.('NAVIGATION');
    await f.settled();
    expect(f.reconnectState()?.timer).not.toBeNull();

    // Its retry reads the row and arms the reconnect that replaces it.
    await jest.advanceTimersByTimeAsync(1_000);
    await f.settled();
    await f.runReconnect(3);
    expect(f.engines).toHaveLength(3);
  });
});

// The liveness watchdog reports an engine that still reads READY, so the engine's own unlink can follow it
// inside the same episode, while the timer the watchdog report armed is pending.
describe('a WhatsApp unlink whose session read fails while the episode timer is armed', () => {
  afterEach(() => jest.useRealTimers());

  /** The next engine links a number other than the one the session was bound to. */
  const linkOther = async (f: ReturnType<typeof fixture>): Promise<void> => {
    f.callbacksOf(1).onReady?.(OTHER, 'Other');
    await until(() => f.stored.phone === OTHER || f.stored.status === SessionStatus.FAILED, 'the link to settle');
  };

  it('is handled as an unlink once the row reads, when the report before it could not read the row either', async () => {
    const f = fixture();
    await f.lifecycle.start(ID);
    const first = f.callbacksOf(0);
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask', 'Date'] });

    f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
    f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
    await f.lifecycle.handleEngineDisconnected(ID, f.registry.get(ID)!, WATCHDOG);
    first.onDisconnected?.('LOGOUT');
    await f.settled();
    expect(f.unlinkAudits()).toHaveLength(0);

    // The database answers again: the pending retry handles the unlink and arms the reconnect.
    await jest.advanceTimersByTimeAsync(1_000);
    await f.settled();
    expect(f.unlinkAudits()).toEqual([
      [AuditAction.SESSION_DISCONNECTED, expect.objectContaining({ sessionId: ID, metadata: { reason: 'LOGOUT' } })],
    ]);
    expect(f.stored.phone).toBeNull();

    // The reconnect's engine links whichever number scans its QR, as after any unlink.
    await f.runReconnect(2);
    await linkOther(f);
    expect(f.stored.phone).toBe(OTHER);
    expect(f.stored.status).toBe(SessionStatus.READY);
  });

  it.each(['LOGOUT', 'logged out'])(
    'is recorded at once when a reconnect is armed and the read of a %s report fails',
    async reason => {
      const f = fixture();
      await f.lifecycle.start(ID);
      const first = f.callbacksOf(0);
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask', 'Date'] });

      await f.lifecycle.handleEngineDisconnected(ID, f.registry.get(ID)!, WATCHDOG);
      f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
      first.onDisconnected?.(reason);
      await f.settled();

      expect(f.unlinkAudits()).toHaveLength(1);
      expect(f.stored.phone).toBeNull();
      expect(f.reconnectState()?.session?.phone).toBeNull();

      // The armed reconnect brings up one engine, which links whichever number scans its QR.
      await f.runReconnect(2);
      await linkOther(f);
      expect(f.stored.phone).toBe(OTHER);
      expect(f.stored.status).toBe(SessionStatus.READY);
      expect(f.engines).toHaveLength(2);
      expect(f.unlinkAudits()).toHaveLength(1);
    },
  );

  it('is audited once when it is reported again while its own read retry waits', async () => {
    const f = fixture();
    await f.lifecycle.start(ID);
    const first = f.callbacksOf(0);
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask', 'Date'] });

    f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
    f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
    first.onDisconnected?.('LOGOUT');
    await f.settled();
    first.onDisconnected?.('LOGOUT');
    await f.settled();
    await jest.advanceTimersByTimeAsync(1_000);
    await f.settled();
    await f.runReconnect(2);

    expect(f.unlinkAudits()).toHaveLength(1);
    expect(f.stored.phone).toBeNull();
    expect(f.engines).toHaveLength(2);
  });

  it('keeps the binding on the row and on the armed reconnect when this node no longer owns the session', async () => {
    const f = fixture();
    await f.lifecycle.start(ID);
    const first = f.callbacksOf(0);
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask', 'Date'] });

    await f.lifecycle.handleEngineDisconnected(ID, f.registry.get(ID)!, WATCHDOG);
    f.disown();
    f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
    first.onDisconnected?.('LOGOUT');
    await f.settled();

    // The audit row is not fenced, as on the path whose read answers; the phone clear is.
    expect(f.unlinkAudits()).toHaveLength(1);
    expect(f.repository.update).not.toHaveBeenCalledWith(ID, { phone: null });
    expect(f.stored.phone).toBe(BOUND);
    expect(f.reconnectState()?.session?.phone).toBe(BOUND);
  });

  it('is ignored when its engine was replaced before the read failed', async () => {
    const f = fixture();
    await f.lifecycle.start(ID);
    const first = f.callbacksOf(0);
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask', 'Date'] });

    await f.lifecycle.handleEngineDisconnected(ID, f.registry.get(ID)!, WATCHDOG);
    let fail!: (error: Error) => void;
    f.repository.findOne.mockImplementationOnce(() => new Promise<Session>((_resolve, reject) => (fail = reject)));
    first.onDisconnected?.('LOGOUT');

    // The reconnect replaces the engine, and the new one drops, which arms the next reconnect.
    await f.runReconnect(2);
    f.callbacksOf(1).onDisconnected?.('NAVIGATION');
    await until(() => f.reconnectState()?.timer != null, 'the next reconnect to be armed');

    fail(new Error('SQLITE_BUSY: database is locked'));
    await f.settled();
    expect(f.unlinkAudits()).toHaveLength(0);
    expect(f.stored.phone).toBe(BOUND);
    expect(f.reconnectState()?.session?.phone).toBe(BOUND);
  });

  it('leaves the binding alone for a drop whose read was retried', async () => {
    const f = fixture();
    await f.lifecycle.start(ID);
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask', 'Date'] });

    f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
    await f.lifecycle.handleEngineDisconnected(ID, f.registry.get(ID)!, WATCHDOG);
    await jest.advanceTimersByTimeAsync(1_000);
    await f.settled();

    expect(f.unlinkAudits()).toHaveLength(0);
    expect(f.stored.phone).toBe(BOUND);
    expect(f.reconnectState()?.session?.phone).toBe(BOUND);
  });

  it('clears the armed reconnect binding even when the row write fails', async () => {
    const f = fixture();
    await f.lifecycle.start(ID);
    const first = f.callbacksOf(0);
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask', 'Date'] });
    const warn = jest.spyOn(
      (f.lifecycle as unknown as { logger: { warn: (message: string, meta?: unknown) => void } }).logger,
      'warn',
    );

    await f.lifecycle.handleEngineDisconnected(ID, f.registry.get(ID)!, WATCHDOG);
    f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
    f.repository.update.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
    first.onDisconnected?.('LOGOUT');
    await f.settled();
    await until(
      () => warn.mock.calls.some(([message]) => message === 'Failed to clear phone after a terminal unlink'),
      'the warning',
    );

    expect(f.stored.phone).toBe(BOUND);
    expect(f.reconnectState()?.session?.phone).toBeNull();
    await f.runReconnect(2);
    await linkOther(f);
    expect(f.stored.phone).toBe(OTHER);
    expect(f.stored.status).toBe(SessionStatus.READY);
  });

  it('leaves a drop whose read fails to the reconnect already armed', async () => {
    const f = fixture();
    await f.lifecycle.start(ID);
    const first = f.callbacksOf(0);
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask', 'Date'] });

    await f.lifecycle.handleEngineDisconnected(ID, f.registry.get(ID)!, WATCHDOG);
    const armed = f.reconnectState()?.timer;
    f.repository.findOne.mockRejectedValueOnce(new Error('SQLITE_BUSY: database is locked'));
    first.onDisconnected?.('NAVIGATION');
    await f.settled();

    expect(f.reconnectState()?.timer).toBe(armed);
    await f.runReconnect(2);
    expect(f.engines).toHaveLength(2);
    expect(f.unlinkAudits()).toHaveLength(0);
    expect(f.stored.phone).toBe(BOUND);
  });

  it('arms nothing for a read that fails after the session was stopped', async () => {
    const f = fixture();
    await f.lifecycle.start(ID);
    const first = f.callbacksOf(0);
    let fail!: (error: Error) => void;
    f.repository.findOne.mockImplementationOnce(() => new Promise<Session>((_resolve, reject) => (fail = reject)));
    first.onDisconnected?.('LOGOUT');

    await f.lifecycle.stop(ID);
    fail(new Error('SQLITE_BUSY: database is locked'));
    await f.settled();

    expect(f.reconnectState()).toBeUndefined();
    expect(f.unlinkAudits()).toHaveLength(0);
    expect(f.stored.phone).toBe(BOUND);
  });
});
