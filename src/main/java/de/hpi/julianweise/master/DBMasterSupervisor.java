package de.hpi.julianweise.master;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.csv.CSVRow;
import de.hpi.julianweise.entity.DatabaseEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

public class DBMasterSupervisor extends AbstractBehavior<DBMasterSupervisor.Response> {

    public interface Response {
    }

    @AllArgsConstructor
    @Getter
    public static class ErrorResponse implements Response {
        private String errorMessage;
    }

    public static Behavior<Response> create(MasterConfiguration configuration) {
        return Behaviors.setup(context -> new DBMasterSupervisor(context, configuration));
    }

    private final ClusterSharding sharding;
    private final ActorRef<CSVParsingActor.Command> csvParser;
    private final MasterConfiguration configuration;
    private final Map<String, DatabaseEntity.UpdateOperation> unconfirmedOperations = new HashMap<>();
    private final ActorRef<ShardingEnvelope<DatabaseEntity.Operation>> entityRef;

    public DBMasterSupervisor(ActorContext<Response> context, MasterConfiguration configuration) {
        super(context);
        context.getLog().info("DBMaster started");

        this.configuration = configuration;
        this.sharding = ClusterSharding.get(context.getSystem());

        this.csvParser = context.spawn(CSVParsingActor.create(), "CSVParser");
        this.csvParser.tell(CSVParsingActor.OpenCSVForParsing.builder()
                                                             .filePath(this.configuration.inputFile.toString())
                                                             .client(context.getSelf())
                                                             .build());

        this.entityRef = sharding.init(Entity.of(DatabaseEntity.ENTITY_TYPE_KEY, ctx -> DatabaseEntity.create()));
    }

    @Override
    public Receive<Response> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, signal -> onPostStop())
                .onMessage(CSVParsingActor.CSVReadyForParsing.class, this::handleCSVReadyForParsing)
                .onMessage(CSVParsingActor.CSVChunk.class, this::handleCSVChunk)
                .onMessage(ErrorResponse.class, this::handleErrorResponse)
                .onMessage(DatabaseEntity.OperationSuccessful.class, this::handleOperationSuccessful)
                .build();
    }

    private DBMasterSupervisor onPostStop() {
        this.getContext().getLog().info("DBMaster stopped");
        return this;
    }

    private Behavior<Response> handleCSVReadyForParsing(CSVParsingActor.CSVReadyForParsing response) {
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.getContext().getSelf()));
        return this;
    }

    private Behavior<Response> handleCSVChunk(CSVParsingActor.CSVChunk chunk) {
        if (chunk.getChunk().size() < 1) {
            return this;
        }
        for (CSVRow row : chunk.getChunk()) {
            DatabaseEntity.UpdateOperation operation = DatabaseEntity.UpdateOperation.builder()
                                                                                     .client(this.getContext().getSelf())
                                                                                     .tuples(row.getTuples())
                                                                                     .id(row.getId())
                                                                                     .build();
            this.unconfirmedOperations.put(row.getId(), operation);
            entityRef.tell(ShardingEnvelope.apply(row.getId(), operation));
        }
        return this;
    }

    private boolean isOperationComplete() {
        return this.unconfirmedOperations.isEmpty();
    }

    private Behavior<Response> handleOperationSuccessful(DatabaseEntity.OperationSuccessful operation) {
        this.unconfirmedOperations.remove(operation.getId());
        if (this.isOperationComplete()) {
            this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.getContext().getSelf()));
        }
        return this;
    }

    private Behavior<Response> handleErrorResponse(ErrorResponse errorResponse) {
        this.getContext().getLog().error(String.format("Unable to proceed: %s", errorResponse.getErrorMessage()));
        return this;
    }
}
