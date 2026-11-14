import { Socket } from "net";
import streamDeck from "@elgato/streamdeck";

/** A single measurement returned by the power meter. */
export interface Response {
  voltage: number;
  current: number;
  power: number;
  timestamp: number;
}

/**
 * The current measurement of a meter, or `undefined` when the meter could not
 * be read: it was unreachable, did not answer, or answered with something that
 * could not be read. No earlier measurement is kept.
 */
export type Reading = Response | undefined;

/** What a subscriber is told whenever the meter has something new to show. */
export type Listener = (reading: Reading) => void;

/** Length in bytes of a measurement response. */
const RESPONSE_SIZE = 30;

/** First five bytes of every measurement response. */
const RESPONSE_HEADER = Buffer.from([0xaa, 0x00, 0x1a, 0x18, 0x00]);

/** Measurement request. */
const REQUEST = Buffer.from([0xaa, 0x00, 0x02, 0x18, 0x00, 0x65]);

/**
 * A power meter, one instance per host.
 *
 * The meter closes the connection itself, both right after answering a request
 * and when a connection stays idle for a second, so a socket is never kept
 * open: each poll connects, sends the request immediately, and disconnects as
 * soon as the response has been read.
 *
 * Actions that use the same host share this instance, so they share the poll
 * instead of issuing a request each: one request is in flight at a time and
 * its measurement is handed to every subscriber.
 */
export class PowerMeter {
  /** TCP port the meter listens on. */
  private static readonly PORT = 60121;

  /** Interval between measurement requests, in milliseconds. */
  private static readonly POLL_INTERVAL = 1000;

  /** Time to wait for the connection and its response, in milliseconds. */
  private static readonly REQUEST_TIMEOUT = 3000;

  /** Cached instances, keyed by host. */
  private static readonly instances = new Map<string, PowerMeter>();

  /**
   * Returns the instance for `host`, creating it on first use. The same host
   * always resolves to the same instance, and therefore to the same poll.
   */
  public static for(host: string): PowerMeter {
    const existing = this.instances.get(host);
    if (existing) {
      return existing;
    }
    const created = new PowerMeter(host);
    this.instances.set(host, created);
    return created;
  }

  /** Listeners, keyed by action context. */
  private readonly listeners = new Map<string, Listener>();

  /** The socket of the request currently in flight, if any. */
  private socket?: Socket;

  /** Response bytes received so far; the response can arrive in several chunks. */
  private buffer: Buffer<ArrayBufferLike> = Buffer.alloc(0);

  /** True while a request is in flight: no other request may be sent. */
  private pending = false;

  /** True once the current trouble was reported, so it is not reported on every poll. */
  private troubleReported = false;

  /** Timer that gives up on the request currently in flight. */
  private requestTimer?: NodeJS.Timeout;

  /** Timer that performs the periodic poll. */
  private pollTimer?: NodeJS.Timeout;

  private constructor(private readonly host: string) { }

  /**
   * Registers `listener`, which is given every measurement the meter answers
   * with, and `undefined` when the meter cannot be read. The first poll runs
   * right away, so a new subscriber hears from the meter immediately.
   * @returns a function that removes the subscription.
   */
  public subscribe(id: string, listener: Listener): () => void {
    this.listeners.set(id, listener);
    this.startPolling();
    return () => this.unsubscribe(id);
  }

  private unsubscribe(id: string): void {
    this.listeners.delete(id);
    if (this.listeners.size === 0) {
      // Nobody is displaying this meter: stop polling and drop the request in flight.
      this.stopPolling();
      this.end(this.socket);
      this.troubleReported = false;
    }
  }

  private startPolling(): void {
    if (this.pollTimer) {
      return;
    }
    this.pollTimer = setInterval(() => this.poll(), PowerMeter.POLL_INTERVAL);
    // Poll right away instead of waiting for the first interval.
    this.poll();
  }

  private stopPolling(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = undefined;
    }
  }

  /**
   * Starts one measurement request when no request is in flight and the meter
   * has subscribers. The connection is closed as soon as the response is read.
   */
  private poll(): void {
    if (this.listeners.size === 0 || this.pending) {
      return;
    }

    this.pending = true;
    this.buffer = Buffer.alloc(0);

    const socket = new Socket();
    this.socket = socket;
    // Why the attempt is going wrong, when it is; reported once the attempt ends.
    let failure: string | undefined;

    socket.setNoDelay(true);

    // Guards the whole attempt: connecting, the request and the response. The
    // meter answers in well under a second and drops idle connections, so a
    // connection that produced nothing by now is of no use.
    this.requestTimer = setTimeout(() => {
      if (this.end(socket)) {
        this.reportFailure("the meter did not answer in time");
      }
    }, PowerMeter.REQUEST_TIMEOUT);

    socket.connect(PowerMeter.PORT, this.host, () => {
      // Send the request without delay: the meter drops a connection that
      // stays idle for a second.
      socket.write(REQUEST, (err) => {
        if (err && this.end(socket)) {
          this.reportFailure(`could not send the request: ${err.message}`);
        }
      });
    });

    socket.on("data", (chunk) => this.onData(socket, chunk));

    socket.on("error", (err) => {
      // Ends the connection, so the close handler below reports it.
      failure = `socket error: ${err.message}`;
    });

    // The meter closes the connection after responding, so when the response
    // was read already, this ends nothing and reports nothing.
    socket.on("close", () => {
      if (this.end(socket)) {
        this.reportFailure(failure ?? "the meter closed the connection without answering");
      }
    });
  }

  private onData(socket: Socket, chunk: Buffer): void {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);

    if (this.buffer.length < RESPONSE_SIZE) {
      return;
    }

    let response: Response;
    try {
      response = parseResponse(this.buffer.subarray(0, RESPONSE_SIZE));
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (this.end(socket)) {
        this.reportFailure(`the meter sent an unreadable response: ${message}`);
      }
      return;
    }

    // The measurement is read; the rest of the stream, if any, is irrelevant.
    if (this.end(socket)) {
      this.troubleReported = false;
      this.notify(response);
    }
  }

  /**
   * Ends the request on `socket` and disconnects, reporting whether this call
   * is the one that ended it. Called when the response is read and on every
   * failure, so a socket is never left open between polls.
   */
  private end(socket?: Socket): boolean {
    if (!socket || this.socket !== socket) {
      return false; // the request ended already
    }
    this.socket = undefined;
    this.buffer = Buffer.alloc(0);
    this.pending = false;
    if (this.requestTimer) {
      clearTimeout(this.requestTimer);
      this.requestTimer = undefined;
    }
    socket.removeAllListeners();
    socket.destroy();
    return true;
  }

  /**
   * Notes that the meter could not be read, and passes nothing to the
   * subscribers once, so the same trouble is not repeated on every poll.
   */
  private reportFailure(reason: string): void {
    streamDeck.logger.error(`[${this.host}] No measurement: ${reason}.`);
    if (this.troubleReported) {
      return;
    }
    this.troubleReported = true;
    this.notify(undefined);
  }

  private notify(reading: Reading): void {
    for (const [id, listener] of this.listeners) {
      try {
        listener(reading);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        streamDeck.logger.error(`Failed to update action ${id}: ${message}`);
      }
    }
  }
}

function parseResponse(data: Buffer): Response {
  if (data.byteLength !== RESPONSE_SIZE) {
    throw Error(`Invalid response length: ${data.byteLength}`);
  }
  if (!data.subarray(0, 5).equals(RESPONSE_HEADER)) {
    throw Error("Invalid response header");
  }
  return {
    voltage: data.readUIntLE(5, 6) / (1 << 24),
    current: data.readUIntLE(11, 6) / (1 << 30),
    power: data.readUIntLE(17, 6) / (1 << 24),
    timestamp: data.readUIntLE(23, 6),
  };
}
