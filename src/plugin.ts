import streamDeck from "@elgato/streamdeck";

import { Measurement } from "./actions/measurement";

// We can enable "trace" logging so that all messages between the Stream Deck, and the plugin are recorded. When storing sensitive information
streamDeck.logger.setLevel("trace");

// Register the increment action.
streamDeck.actions.registerAction(new Measurement());

// Finally, connect to the Stream Deck.
streamDeck.connect();
