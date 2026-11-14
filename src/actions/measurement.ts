import { action, DidReceiveSettingsEvent, KeyAction, SingletonAction, WillAppearEvent, WillDisappearEvent } from "@elgato/streamdeck";
import streamDeck from "@elgato/streamdeck";
import { PowerMeter, type Reading } from "../power-meter.js";

@action({ UUID: "com.nkyoku.wfwattch2-client.measurement" })
export class Measurement extends SingletonAction<MeasurementSettings> {
  /** Visible keys of this action, mapped to the function that releases their subscription. */
  private readonly subscriptions = new Map<string, () => void>();

  /**
   * Called when a key becomes visible. The key subscribes to the shared
   * {@link PowerMeter} for its host, which polls the meter on a schedule and
   * hands every key the measurement; the key never sends a request of its own.
   */
  override async onWillAppear(ev: WillAppearEvent<MeasurementSettings>): Promise<void> {
    if (!ev.action.isKey()) {
      return;
    }
    this.subscribe(ev.action, ev.payload.settings);
  }

  override async onWillDisappear(ev: WillDisappearEvent<MeasurementSettings>): Promise<void> {
    this.unsubscribe(ev.action.id);
  }

  override async onDidReceiveSettings(ev: DidReceiveSettingsEvent<MeasurementSettings>): Promise<void> {
    if (!ev.action.isKey()) {
      return;
    }
    // The host may have changed: subscribe to the meter for the new settings.
    this.subscribe(ev.action, ev.payload.settings);
  }

  /** Registers `key` with the meter for its host, replacing any previous subscription of that key. */
  private subscribe(key: KeyAction<MeasurementSettings>, settings: MeasurementSettings): void {
    const id = key.id;
    this.unsubscribe(id);

    const host = settings.host?.trim();
    if (!host) {
      void this.setTitle(key, "?");
      return;
    }

    // Keys using the same host share one instance, one poll and one request per measurement.
    const unsubscribe = PowerMeter.for(host).subscribe(id, (reading) => {
      void this.update(key, settings, reading);
    });

    this.subscriptions.set(id, unsubscribe);
  }

  private unsubscribe(id: string): void {
    const previous = this.subscriptions.get(id);
    if (previous) {
      this.subscriptions.delete(id);
      previous();
    }
  }

  /** Writes the value the meter reported to the title of `key`. */
  private async update(key: KeyAction<MeasurementSettings>, settings: MeasurementSettings, reading: Reading): Promise<void> {
    let value = reading?.power;
    let unit = "W";
    switch (settings.type) {
      case "voltage":
        value = reading?.voltage;
        unit = "V";
        break;
      case "current":
        value = reading?.current;
        unit = "A";
        break;
    }
    await this.setTitle(key, `${value?.toFixed(0) ?? "?"}\n${unit}`);
  }

  private async setTitle(key: KeyAction<MeasurementSettings>, title: string): Promise<void> {
    try {
      await key.setTitle(title);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      streamDeck.logger.error(`Failed to set title: ${message}`);
    }
  }
}

type MeasurementSettings = {
  host?: string;
  type: "voltage" | "current" | "power";
};
