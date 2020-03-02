package de.hpi.julianweise.master;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.shard.ADBShardDistributor;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class ADBLoadAndDistributeDataProcess extends AbstractBehavior<ADBLoadAndDistributeDataProcess.Command> {

    public interface Command {
    }

    @AllArgsConstructor
    @Getter
    public static class WrappedCSVParserResponse implements Command {
        private CSVParsingActor.Response response;
    }

    @AllArgsConstructor
    @Getter
    public static class WrappedShardDistributorResponse implements Command {
        private ADBShardDistributor.Response response;
    }

    @AllArgsConstructor
    @Getter
    public static class Start implements Command {
        ActorRef<ADBMasterSupervisor.Command> respondTo;
    }

    private final ActorRef<CSVParsingActor.Command> csvParser;
    private final ActorRef<ADBShardDistributor.Command> shardDistributor;
    private final ActorRef<CSVParsingActor.Response> csvResponseWrapper;
    private final ActorRef<ADBShardDistributor.Response> shardDistributorWrapper;
    private ActorRef<ADBMasterSupervisor.Command> client;


    protected ADBLoadAndDistributeDataProcess(ActorContext<Command> context,
                                              Behavior<CSVParsingActor.Command> csvParser,
                                              Behavior<ADBShardDistributor.Command> shardDistributor) {
        super(context);
        this.csvParser = context.spawn(csvParser, "CSVParser");
        this.shardDistributor = context.spawn(shardDistributor, "ShardDistributor");
        this.csvResponseWrapper = this.getContext().messageAdapter(CSVParsingActor.Response.class,
                WrappedCSVParserResponse::new);
        this.shardDistributorWrapper = this.getContext().messageAdapter(ADBShardDistributor.Response.class,
                WrappedShardDistributorResponse::new);
    }


    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Start.class, this::handleStart)
                .onMessage(WrappedCSVParserResponse.class, this::handleCSVParserResponse)
                .onMessage(WrappedShardDistributorResponse.class, this::handleShardDistributorResponse)
                .build();
    }

    private Behavior<Command> handleStart(Start command) {
        this.client = command.getRespondTo();
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.csvResponseWrapper));
        return Behaviors.same();
    }

    private Behavior<Command> handleCSVParserResponse(WrappedCSVParserResponse response) {
        if (response.getResponse() instanceof CSVParsingActor.DomainDataChunk) {
            return this.handleCSVChunk((CSVParsingActor.DomainDataChunk) response.getResponse());
        } else if (response.getResponse() instanceof CSVParsingActor.CSVFullyParsed) {
            return this.handleCSVFullyParsed((CSVParsingActor.CSVFullyParsed) response.getResponse());
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleShardDistributorResponse(WrappedShardDistributorResponse response) {
        if (response.getResponse() instanceof ADBShardDistributor.BatchDistributed) {
            return this.handleBatchDistributed((ADBShardDistributor.BatchDistributed) response.getResponse());
        }
        if (response.getResponse() instanceof ADBShardDistributor.DataFullyDistributed) {
            return this.handleDataFullyDistributed((ADBShardDistributor.DataFullyDistributed) response.getResponse());
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleCSVChunk(CSVParsingActor.DomainDataChunk chunk) {
        this.shardDistributor.tell(new ADBShardDistributor.DistributeBatch(this.shardDistributorWrapper,
                chunk.getChunk()));
        return Behaviors.same();
    }

    private Behavior<Command> handleBatchDistributed(ADBShardDistributor.BatchDistributed command) {
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.csvResponseWrapper));
        return Behaviors.same();
    }

    private Behavior<Command> handleCSVFullyParsed(CSVParsingActor.CSVFullyParsed command) {
        this.shardDistributor.tell(new ADBShardDistributor.ConcludeDistribution());
        this.getContext().stop(this.csvParser);
        return Behaviors.same();
    }

    private Behavior<Command> handleDataFullyDistributed(ADBShardDistributor.DataFullyDistributed response) {
        this.getContext().getLog().info("### Data have been fully loaded into the database ###");
        this.client.tell(new ADBMasterSupervisor.StartOperationalService());
        this.getContext().stop(this.shardDistributor);
        System.gc();
        return Behaviors.same();
    }

}
