package de.hpi.julianweise.master;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.domain.ADBEntityFactory;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.shard.ADBShardDistributor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

public class ADBMasterSupervisor extends AbstractBehavior<ADBMasterSupervisor.Response> {

    public interface Response {
    }

    @AllArgsConstructor
    @Getter
    public static class ErrorResponse implements Response {
        private String errorMessage;
    }

    @AllArgsConstructor
    @Getter
    @Builder
    public static class ParseAndDistributeCSV implements Response {
        private String filePath;
        private ADBEntityFactory entityFactory;
    }

    @AllArgsConstructor
    @Getter
    public static class DataSuccessfullyDistributed implements Response {
    }

    public static Behavior<Response> create(MasterConfiguration configuration ) {
        return Behaviors.setup(context -> new ADBMasterSupervisor(context, configuration));
    }

    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final MasterConfiguration configuration;
    private ActorRef<CSVParsingActor.Command> csvParser;
    private ActorRef<ADBShardDistributor.Command> shardDistributor;

    private ADBMasterSupervisor(ActorContext<Response> context, MasterConfiguration configuration) {
        super(context);
        context.getLog().info("DBMaster started");

        this.configuration = configuration;
    }

    @Override
    public Receive<Response> createReceive() {
        return newReceiveBuilder()
                .onMessage(ParseAndDistributeCSV.class, this::handleParseAndDistributeCSV)
                .onMessage(DataSuccessfullyDistributed.class, this::handleDataSuccessfullyDistributed)
                .onMessage(CSVParsingActor.CSVReadyForParsing.class, this::handleCSVReadyForParsing)
                .onMessage(CSVParsingActor.DomainDataChunk.class, this::handleCSVChunk)
                .onMessage(ADBShardDistributor.BatchDistributed.class, this::handleBatchDistributed)
                .onMessage(CSVParsingActor.CSVFullyParsed.class, this::handleCSVFullyParsed)
                .onMessage(ErrorResponse.class, this::handleErrorResponse)
                .build();
    }

    private Behavior<Response> handleParseAndDistributeCSV(ParseAndDistributeCSV command) {
        if (this.isProcessingAndDistributingData()) {
            this.getContext().getLog().warn("Received request to handle CSV although already processing data");
            return this;
        }

        this.csvParser = this.getContext().spawn(CSVParsingActor.create(), "CSVParser");
        this.shardDistributor = this.getContext().spawn(ADBShardDistributor.create(), "ShardDistributor");

        this.csvParser.tell(CSVParsingActor.OpenCSVForParsing
                .builder()
                .filePath(this.configuration.inputFile.toString())
                .chunkSize(this.settings.CSV_CHUNK_SIZE)
                .domainFactory(command.getEntityFactory())
                .client(this.getContext().getSelf())
                .build());
        return this;
    }

    private Behavior<Response> handleCSVReadyForParsing(CSVParsingActor.CSVReadyForParsing response) {
        if (!this.isProcessingAndDistributingData()) {
            this.getContext().getLog().warn("Got notified about csv ready for parsing although not processing data");
            return this;
        }
        response.getRespondTo().tell(new CSVParsingActor.ParseNextCSVChunk(this.getContext().getSelf()));
        return this;
    }

    private Behavior<Response> handleCSVChunk(CSVParsingActor.DomainDataChunk chunk) {
        if (!this.isProcessingAndDistributingData()) {
            this.getContext().getLog().warn("Got csv data chunk although not processing data");
            return this;
        }
        this.shardDistributor.tell(new ADBShardDistributor.DistributeBatchToShards(this.getContext().getSelf(),
                chunk.getChunk()));
        return this;
    }

    private Behavior<Response> handleErrorResponse(ErrorResponse errorResponse) {
        this.getContext().getLog().error(String.format("Unable to proceed: %s", errorResponse.getErrorMessage()));
        return this;
    }

    private Behavior<Response> handleBatchDistributed(ADBShardDistributor.BatchDistributed command) {
        if (!this.isProcessingAndDistributingData()) {
            return this;
        }
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.getContext().getSelf()));
        return this;
    }

    private Behavior<Response> handleCSVFullyParsed(CSVParsingActor.CSVFullyParsed command) {
        this.getContext().getLog().info("Finished parsing CSV file");
        this.shardDistributor.tell(new ADBShardDistributor.ConcludeDistribution());
        this.getContext().stop(this.csvParser);
        this.csvParser = null;
        return this;
    }

    private Behavior<Response> handleDataSuccessfullyDistributed(DataSuccessfullyDistributed response) {
        this.getContext().getLog().info("Successfully distributed all data");
        this.getContext().stop(this.shardDistributor);
        this.shardDistributor = null;
        System.gc();
        return this;
    }

    private boolean isProcessingAndDistributingData() {
        return this.csvParser != null && this.shardDistributor != null;
    }
}
