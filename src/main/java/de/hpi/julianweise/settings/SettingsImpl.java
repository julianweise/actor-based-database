package de.hpi.julianweise.settings;

import akka.actor.Extension;
import com.typesafe.config.Config;

public class SettingsImpl implements Extension {

    public final int CSV_CHUNK_SIZE;
    public final String ENDPOINT_HOSTNAME;
    public final int ENDPOINT_PORT;
    public final int NUMBER_OF_THREADS;

    public SettingsImpl(Config config) {
        CSV_CHUNK_SIZE = config.getInt("actor-db.csv.chunk-size");
        NUMBER_OF_THREADS = config.getInt("actor-db.number-of-threads");
        ENDPOINT_HOSTNAME = config.getString("actor-db.query-endpoint.hostname");
        ENDPOINT_PORT = config.getInt("actor-db.query-endpoint.port");
    }
}