package de.hpi.julianweise.settings;

import akka.actor.Extension;
import com.typesafe.config.Config;

public class SettingsImpl implements Extension {

    public final int CSV_CHUNK_SIZE;
    public final String ENDPOINT_HOSTNAME;
    public final int ENDPOINT_PORT;
    public final int NUMBER_OF_THREADS;
    public final int MAX_SIZE_PARTITION;
    public final double JOIN_STRATEGY_LOWER_BOUND;
    public final double JOIN_STRATEGY_UPPER_BOUND;
    public final int PARALLEL_PARTITION_JOINS;
    public final String RESULT_BASE_DIR;
    public final int THRESHOLD_NEXT_NODE_COMPARISON;
    public final int NUMBER_ENTITY_CONVERTER;
    public final int NUMBER_DISTRIBUTOR;

    public SettingsImpl(Config config) {
        CSV_CHUNK_SIZE = config.getInt("actor-db.data-loading.csv.chunk-size");
        NUMBER_OF_THREADS = config.getInt("actor-db.number-of-threads");
        ENDPOINT_HOSTNAME = config.getString("actor-db.query-endpoint.hostname");
        ENDPOINT_PORT = config.getInt("actor-db.query-endpoint.port");
        JOIN_STRATEGY_LOWER_BOUND = config.getDouble("actor-db.join.strategy.lower-bound");
        JOIN_STRATEGY_UPPER_BOUND = config.getDouble("actor-db.join.strategy.upper-bound");
        MAX_SIZE_PARTITION = config.getInt("actor-db.data-loading.partition.size");
        PARALLEL_PARTITION_JOINS = config.getInt("actor-db.number-of-parallel-partition-joins");
        RESULT_BASE_DIR = config.getString("actor-db.results.dir");
        THRESHOLD_NEXT_NODE_COMPARISON = config.getInt("actor-db.join.strategy.threshold-request-next-node");
        NUMBER_ENTITY_CONVERTER = config.getInt("actor-db.data-loading.entity-converter.number");
        NUMBER_DISTRIBUTOR = config.getInt("actor-db.data-loading.distributor.number");
    }
}