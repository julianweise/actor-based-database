package de.hpi.julianweise.settings;

import akka.actor.Extension;
import com.typesafe.config.Config;

import static java.lang.Integer.parseInt;

public class SettingsImpl implements Extension {

    public final int CSV_CHUNK_SIZE;

    public SettingsImpl(Config config) {
        CSV_CHUNK_SIZE = parseInt(config.getString("actor-db.csv.chunk-size"), 10);
    }
}