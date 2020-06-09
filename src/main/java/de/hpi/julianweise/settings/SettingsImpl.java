package de.hpi.julianweise.settings;

import akka.actor.Extension;
import com.typesafe.config.Config;

public class SettingsImpl implements Extension {

    public final int CSV_CHUNK_SIZE;
    public final String ENDPOINT_HOSTNAME;
    public final int ENDPOINT_PORT;
    public final int NUMBER_OF_THREADS;
    public final int JOIN_ROW_CHUNK_SIZE;
    public final int MAX_SIZE_PARTITION;
    public final double JOIN_STRATEGY_LOWER_BOUND;
    public final double JOIN_STRATEGY_UPPER_BOUND;
    public final int PARALLEL_PARTITION_JOINS;
    public final String RESULT_BASE_DIR;

    public SettingsImpl(Config config) {
        CSV_CHUNK_SIZE = config.getInt("actor-db.csv.chunk-size");
        NUMBER_OF_THREADS = config.getInt("actor-db.number-of-threads");
        ENDPOINT_HOSTNAME = config.getString("actor-db.query-endpoint.hostname");
        ENDPOINT_PORT = config.getInt("actor-db.query-endpoint.port");
        JOIN_STRATEGY_LOWER_BOUND = config.getDouble("actor-db.join.strategy.lower-bound");
        JOIN_STRATEGY_UPPER_BOUND = config.getDouble("actor-db.join.strategy.upper-bound");
        JOIN_ROW_CHUNK_SIZE = config.getInt("actor-db.join.row.chunk-size");
        MAX_SIZE_PARTITION = config.getInt("actor-db.partition.size");
        PARALLEL_PARTITION_JOINS = config.getInt("actor-db.number-of-parallel-partition-joins");
        RESULT_BASE_DIR = config.getString("actor-db.results.dir");
    }
}