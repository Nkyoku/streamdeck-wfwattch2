import { action, DidReceiveSettingsEvent, KeyDownEvent, SingletonAction, WillAppearEvent } from "@elgato/streamdeck";
import streamDeck from "@elgato/streamdeck";
import { Socket } from "net";

interface Response {
  voltage: number;
  current: number;
  power: number;
  timestamp: number;
};

function parseResponse(data: Buffer<ArrayBufferLike>): Response {
  if (data.byteLength !== 30) {
    throw Error(`Invalid response length: ${data.byteLength}`);
  }
  if (data.readUIntBE(0, 5) !== 0xAA001A1800) {
    throw Error("Invalid response header");
  }
  return {
    voltage: data.readUIntLE(5, 6) / (1 << 24),
    current: data.readUIntLE(11, 6) / (1 << 30),
    power: data.readUIntLE(17, 6) / (1 << 24),
    timestamp: data.readUIntLE(23, 6),
  };
}

@action({ UUID: "com.nkyoku.wfwattch2-client.power" })
export class Measurement extends SingletonAction<MeasurementSettings> {
  constructor() {
    super();
  }

  override async onWillAppear(ev: WillAppearEvent<MeasurementSettings>): Promise<void> {
    if (!ev.action.isKey()) {
      return;
    }
    await ev.action.setTitle(`${ev.payload.settings.host ?? "none"}`);
  }

  override async onDidReceiveSettings(ev: DidReceiveSettingsEvent<MeasurementSettings>): Promise<void> {
    if (!ev.action.isKey()) {
      return;
    }
    await ev.action.setTitle(`${ev.payload.settings.host ?? "none"}`);
  }

  override async onKeyDown(ev: KeyDownEvent<MeasurementSettings>): Promise<void> {
    const { settings } = ev.payload;

    if (!settings.host) {
      return;
    }

    const client = new Socket();
    client.connect(60121, settings.host, async () => {
      // Send measurement request
      const data = new Uint8Array([0xAA, 0x00, 0x02, 0x18, 0x00, 0x65]);
      client.write(data);
    });

    client.on("data", async data => {
      try {
        const response = parseResponse(data);
        let value = response.power;
        let unit = "W";
        switch (settings.type) {
          case "voltage":
            value = response.voltage;
            unit = "V";
            break;
          case "current":
            value = response.current;
            unit = "A";
            break;
        }
        await ev.action.setTitle(`${value?.toFixed(0)}\n${unit}`);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        streamDeck.logger.error(`Failed to parse response: ${message}`);
      }
      client.end();
    });

    client.on("error", err => {
      streamDeck.logger.error(`Socket error: ${err.message}`);
    });
  }

  /*async onTimeout() {
  
  this.actions.forEach(async action => {
  try {
  const settings = await action.getSettings();
  
  
  } catch (err) {
  const message = err instanceof Error ? err.message : String(err);
  streamDeck.logger.error(`could not start plugin: ${ message }`);
  }
  });
  
  setTimeout(async () => { await this.onTimeout(); }, 1000);
  }*/
}

type MeasurementSettings = {
  host?: string;
  type: "voltage" | "current" | "power";
};
