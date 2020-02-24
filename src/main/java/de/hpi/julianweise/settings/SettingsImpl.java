package de.hpi.julianweise.settings;

import akka.actor.AbstractExtensionId;
import akka.actor.ExtendedActorSystem;
import akka.actor.Extension;
import akka.actor.ExtensionIdProvider;
import com.typesafe.config.Config;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;

public class SettingsImpl implements Extension {

    public final int CSV_CHUNK_SIZE;

    public SettingsImpl(Config config) {
        CSV_CHUNK_SIZE = parseInt(config.getString("actor-db.csv.chunk-size"), 10);
    }
}