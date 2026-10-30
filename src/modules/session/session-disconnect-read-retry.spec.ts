import { SessionEngineLifecycle } from './session-engine-lifecycle.service';
import { EngineRegistry } from '../../engine/engine-registry.service';
import { Session, SessionStatus } from './entities/session.entity';
import type { EngineEventCallbacks } from '../../engine/interfaces/whatsapp-engine.interface';

const ID = '66666666-6666-4666-8666-666666666666';
const BOUND = '628111';
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
  };
  const reconnectState = () => internals.reconnectStates.get(ID);
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
