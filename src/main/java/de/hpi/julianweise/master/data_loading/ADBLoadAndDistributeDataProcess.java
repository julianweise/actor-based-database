package de.hpi.julianweise.master.data_loading;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.PoolRouter;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.Routers;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.master.ADBMaster;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributorFactory;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

import java.util.concurrent.atomic.AtomicInteger;

public class ADBLoadAndDistributeDataProcess extends AbstractBehavior<ADBLoadAndDistributeDataProcess.Command> {

    private final ActorRef<CSVParsingActor.Command> csvParser;
    private final ActorRef<ADBDataDistributor.Command> dataDistributor;
    private final ActorRef<CSVParsingActor.Response> csvResponseWrapper;
    private final ActorRef<ADBDataDistributor.Response> dataDistributorWrapper;
    private final ActorRef<ADBCSVToEntityConverter.Response> converterWrapper;
    private final ActorRef<ADBCSVToEntityConverter.Command> entityConverter;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final AtomicInteger distributedBatchBatches = new AtomicInteger(0);
    private final AtomicInteger finalizedConverter = new AtomicInteger(0);
    private final AtomicInteger finalizedDistributor = new AtomicInteger(0);
    private ActorRef<ADBMaster.Command> client;

    public interface Command {}

    @AllArgsConstructor
    @Getter
    public static class WrappedCSVParserResponse implements Command {
        private final CSVParsingActor.Response response;
    }

    @AllArgsConstructor
    @Getter
    public static class WrappedNodeDistributorResponse implements Command {
        private final ADBDataDistributor.Response response;
    }

    @AllArgsConstructor
    @Getter
    public static class WrappedConverterResponse implements Command {
        private final ADBCSVToEntityConverter.Response response;
    }

    @AllArgsConstructor
    @Getter
    public static class Start implements Command {
        private final ActorRef<ADBMaster.Command> respondTo;
    }


    protected ADBLoadAndDistributeDataProcess(ActorContext<Command> context,
                                              Behavior<CSVParsingActor.Command> csvParser) {
        super(context);
        this.csvParser = context.spawn(csvParser, "CSVParser",  DispatcherSelector.fromConfig("io-blocking-dispatcher"));
        this.dataDistributor = this.spawnDistributorPool(context);
        this.entityConverter = this.spawnConverterPool(context);
        this.csvResponseWrapper = this.getCSVResponseWrapper();
        this.dataDistributorWrapper = this.getDataDistributorResponseWrapper();
        this.converterWrapper = this.getConverterWrapper();
    }

    private ActorRef<ADBDataDistributor.Command> spawnDistributorPool(ActorContext<Command> context) {
        PoolRouter<ADBDataDistributor.Command> pool = Routers
                .pool(this.settings.NUMBER_DISTRIBUTOR, Behaviors
                        .supervise(ADBDataDistributorFactory.createDefault())
                        .onFailure(SupervisorStrategy.restart()))
                .withRoundRobinRouting();
        return context.spawn(pool, "distributor-pool");
    }

    private ActorRef<ADBCSVToEntityConverter.Command> spawnConverterPool(ActorContext<Command> context) {
        return context.spawn(Routers.pool(this.settings.NUMBER_ENTITY_CONVERTER, Behaviors
                .supervise(ADBCSVToEntityConverter.createDefault())
                .onFailure(SupervisorStrategy.restart())), "converter-pool");
    }

    private ActorRef<CSVParsingActor.Response> getCSVResponseWrapper() {
        return getContext().messageAdapter(CSVParsingActor.Response.class, WrappedCSVParserResponse::new);
    }

    private ActorRef<ADBDataDistributor.Response> getDataDistributorResponseWrapper() {
        return getContext().messageAdapter(ADBDataDistributor.Response.class, WrappedNodeDistributorResponse::new);
    }

    private ActorRef<ADBCSVToEntityConverter.Response> getConverterWrapper() {
        return getContext().messageAdapter(ADBCSVToEntityConverter.Response.class, WrappedConverterResponse::new);
    }


    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Start.class, this::handleStart)
                .onMessage(WrappedCSVParserResponse.class, this::handleCSVParserResponse)
                .onMessage(WrappedNodeDistributorResponse.class, this::handleDataDistributorResponse)
                .onMessage(WrappedConverterResponse.class, this::handleConverterResponse)
                .build();
    }

    private Behavior<Command> handleStart(Start command) {
        this.client = command.getRespondTo();
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.csvResponseWrapper));
        return Behaviors.same();
    }

    private Behavior<Command> handleCSVParserResponse(WrappedCSVParserResponse response) {
        if (response.getResponse() instanceof CSVParsingActor.CSVDataChunk) {
            return this.handleCSVChunk((CSVParsingActor.CSVDataChunk) response.getResponse());
        } else if (response.getResponse() instanceof CSVParsingActor.CSVFullyParsed) {
            return this.handleCSVFullyParsed();
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleCSVChunk(CSVParsingActor.CSVDataChunk chunk) {
        int chunkSize = (int) Math.ceil((double) chunk.getChunk().size() / this.settings.NUMBER_ENTITY_CONVERTER);
        for (int i = 0; i < chunk.getChunk().size(); i += chunkSize) {
            val payload = chunk.getChunk().subList(i, Math.min(chunk.getChunk().size(), i + chunkSize));
            this.entityConverter.tell(new ADBCSVToEntityConverter.ConvertBatch(this.converterWrapper, payload));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleCSVFullyParsed() {
        this.getContext().stop(this.csvParser);
        for(int i = 0; i < this.settings.NUMBER_ENTITY_CONVERTER; i++) {
            this.entityConverter.tell(new ADBCSVToEntityConverter.Finalize(this.converterWrapper));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleConverterResponse(WrappedConverterResponse cmd) {
        if (cmd.response instanceof ADBCSVToEntityConverter.ConvertedBatch) {
            ObjectList<ADBEntity> entities = ((ADBCSVToEntityConverter.ConvertedBatch) cmd.response).entities;
            dataDistributor.tell(new ADBDataDistributor.DistributeBatch(dataDistributorWrapper, entities));
        } else if (cmd.response instanceof ADBCSVToEntityConverter.Finalized) {
            if (this.finalizedConverter.incrementAndGet() < this.settings.NUMBER_ENTITY_CONVERTER) {
                return Behaviors.same();
            }
            for(int i = 0; i < this.settings.NUMBER_DISTRIBUTOR; i++) {
                this.dataDistributor.tell(new ADBDataDistributor.ConcludeDistribution(this.dataDistributorWrapper));
            }
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleDataDistributorResponse(WrappedNodeDistributorResponse response) {
        if (response.getResponse() instanceof ADBDataDistributor.BatchDistributed) {
            return this.handleBatchDistributed();
        }
        else {
            if (this.finalizedDistributor.incrementAndGet() >= this.settings.NUMBER_DISTRIBUTOR) {
                return this.handleDataFullyDistributed();
            }
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleBatchDistributed() {
        if (distributedBatchBatches.incrementAndGet() < settings.NUMBER_DISTRIBUTOR || finalizedConverter.get() > 0) {
            return Behaviors.same();
        }
        this.distributedBatchBatches.set(0);
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.csvResponseWrapper));
        return Behaviors.same();
    }

    private Behavior<Command> handleDataFullyDistributed() {
        this.getContext().getLog().info("### Data have been fully loaded into the database ###");
        this.client.tell(new ADBMaster.StartOperationalService());
        System.gc();
        return Behaviors.stopped();
    }

}
