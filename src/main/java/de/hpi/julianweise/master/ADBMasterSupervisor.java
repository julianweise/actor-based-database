package de.hpi.julianweise.master;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
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
import lombok.Getter;

public class ADBMasterSupervisor extends AbstractBehavior<ADBMasterSupervisor.Response> {

    public interface Response {
    }

    @AllArgsConstructor
    @Getter
    public static class ErrorResponse implements Response {
        private String errorMessage;
    }

    public static Behavior<Response> create(MasterConfiguration configuration, ADBEntityFactory entityFactory) {
        return Behaviors.setup(context -> new ADBMasterSupervisor(context, configuration, entityFactory));
    }

    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final ActorRef<CSVParsingActor.Command> csvParser;
    private final MasterConfiguration configuration;
    private final ActorRef<ADBShardDistributor.Command> shardDistributor;

    public ADBMasterSupervisor(ActorContext<Response> context, MasterConfiguration configuration, ADBEntityFactory entityFactory) {
        super(context);
        context.getLog().info("DBMaster started");

        this.configuration = configuration;

        this.csvParser = context.spawn(CSVParsingActor.create(), "CSVParser");
        this.csvParser.tell(CSVParsingActor.OpenCSVForParsing.builder()
                                                             .filePath(this.configuration.inputFile.toString())
                                                             .chunkSize(this.settings.CSV_CHUNK_SIZE)
                                                             .domainFactory(entityFactory)
                                                             .client(context.getSelf())
                                                             .build());

        this.shardDistributor = context.spawn(ADBShardDistributor.create(), "ShardDistributor");
    }

    @Override
    public Receive<Response> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, signal -> onPostStop())
                .onMessage(CSVParsingActor.CSVReadyForParsing.class, this::handleCSVReadyForParsing)
                .onMessage(CSVParsingActor.DomainDataChunk.class, this::handleCSVChunk)
                .onMessage(ADBShardDistributor.BatchDistributed.class, this::handleBatchDistributed)
                .onMessage(ErrorResponse.class, this::handleErrorResponse)
                .build();
    }

    private ADBMasterSupervisor onPostStop() {
        this.getContext().getLog().info("DBMaster stopped");
        return this;
    }

    private Behavior<Response> handleCSVReadyForParsing(CSVParsingActor.CSVReadyForParsing response) {
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.getContext().getSelf()));
        return this;
    }

    private Behavior<Response> handleCSVChunk(CSVParsingActor.DomainDataChunk chunk) {
        this.shardDistributor.tell(new ADBShardDistributor.DistributeBatchToShards(this.getContext().getSelf(),
                chunk.getChunk()));
        return this;
    }

    private Behavior<Response> handleErrorResponse(ErrorResponse errorResponse) {
        this.getContext().getLog().error(String.format("Unable to proceed: %s", errorResponse.getErrorMessage()));
        return this;
    }

    private Behavior<Response> handleBatchDistributed(ADBShardDistributor.BatchDistributed command) {
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.getContext().getSelf()));
        return this;
    }
}
