package de.hpi.julianweise.master.data_loading;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.master.ADBMaster;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class ADBLoadAndDistributeDataProcess extends AbstractBehavior<ADBLoadAndDistributeDataProcess.Command> {

    private final ActorRef<CSVParsingActor.Command> csvParser;
    private final ActorRef<ADBDataDistributor.Command> shardDistributor;
    private final ActorRef<CSVParsingActor.Response> csvResponseWrapper;
    private final ActorRef<ADBDataDistributor.Response> shardDistributorWrapper;
    private ActorRef<ADBMaster.Command> client;

    public interface Command {
    }

    @AllArgsConstructor
    @Getter
    public static class WrappedCSVParserResponse implements Command {
        private final CSVParsingActor.Response response;

    }
    @AllArgsConstructor
    @Getter
    public static class WrappedShardDistributorResponse implements Command {
        private final ADBDataDistributor.Response response;

    }
    @AllArgsConstructor
    @Getter
    public static class Start implements Command {
        private final ActorRef<ADBMaster.Command> respondTo;

    }


    protected ADBLoadAndDistributeDataProcess(ActorContext<Command> context,
                                              Behavior<CSVParsingActor.Command> csvParser,
                                              Behavior<ADBDataDistributor.Command> shardDistributor) {
        super(context);
        this.csvParser = context.spawn(csvParser, "CSVParser");
        this.shardDistributor = context.spawn(shardDistributor, "ShardDistributor");
        this.csvResponseWrapper = this.getContext().messageAdapter(CSVParsingActor.Response.class,
                WrappedCSVParserResponse::new);
        this.shardDistributorWrapper = this.getContext().messageAdapter(ADBDataDistributor.Response.class,
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
            return this.handleCSVFullyParsed();
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleShardDistributorResponse(WrappedShardDistributorResponse response) {
        if (response.getResponse() instanceof ADBDataDistributor.BatchDistributed) {
            return this.handleBatchDistributed();
        }
        if (response.getResponse() instanceof ADBDataDistributor.DataFullyDistributed) {
            return this.handleDataFullyDistributed();
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleCSVChunk(CSVParsingActor.DomainDataChunk chunk) {
        this.shardDistributor.tell(new ADBDataDistributor.DistributeBatch(this.shardDistributorWrapper,
                chunk.getChunk()));
        return Behaviors.same();
    }

    private Behavior<Command> handleBatchDistributed() {
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.csvResponseWrapper));
        return Behaviors.same();
    }

    private Behavior<Command> handleCSVFullyParsed() {
        this.shardDistributor.tell(new ADBDataDistributor.ConcludeDistribution());
        this.getContext().stop(this.csvParser);
        return Behaviors.same();
    }

    private Behavior<Command> handleDataFullyDistributed() {
        this.getContext().getLog().info("### Data have been fully loaded into the database ###");
        this.client.tell(new ADBMaster.StartOperationalService());
        this.getContext().stop(this.shardDistributor);
        System.gc();
        return Behaviors.stopped();
    }

}
