package de.hpi.julianweise.settings;

import akka.actor.Extension;
import com.typesafe.config.Config;

public class SettingsImpl implements Extension {

    public final int CSV_CHUNK_SIZE;
    public final String ENDPOINT_HOSTNAME;
    public final int ENDPOINT_PORT;
    public final int NUMBER_OF_THREADS;
    public final int PARTITION_SIZE;
    public final double JOIN_STRATEGY_LOWER_BOUND;
    public final double JOIN_STRATEGY_UPPER_BOUND;
    public final String RESULT_BASE_DIR;
    public final double THRESHOLD_NEXT_NODE_COMPARISON;
    public final int NUMBER_ENTITY_CONVERTER;
    public final int DISTRIBUTION_CHUNK_SIZE;
    public final double WORK_STEALING_AMOUNT;

    public SettingsImpl(Config config) {
        CSV_CHUNK_SIZE = config.getInt("actor-db.data-loading.csv.chunk-size");
        NUMBER_OF_THREADS = config.getInt("actor-db.number-of-threads");
        ENDPOINT_HOSTNAME = config.getString("actor-db.query-endpoint.hostname");
        ENDPOINT_PORT = config.getInt("actor-db.query-endpoint.port");
        JOIN_STRATEGY_LOWER_BOUND = config.getDouble("actor-db.join.strategy.lower-bound");
        JOIN_STRATEGY_UPPER_BOUND = config.getDouble("actor-db.join.strategy.upper-bound");
        PARTITION_SIZE = config.getInt("actor-db.data-loading.partition.size");
        RESULT_BASE_DIR = config.getString("actor-db.results.dir");
        THRESHOLD_NEXT_NODE_COMPARISON = config.getDouble("actor-db.join.strategy.threshold-request-next-node");
        NUMBER_ENTITY_CONVERTER = config.getInt("actor-db.data-loading.entity-converter.number");
        DISTRIBUTION_CHUNK_SIZE = config.getInt("actor-db.data-loading.distributor.chunk-size");
        WORK_STEALING_AMOUNT = config.getDouble("actor-db.work-stealing.amount");
    }
}