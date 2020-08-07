package de.hpi.julianweise.master.data_loading;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.Routers;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.csv.CSVParsingActor;
import de.hpi.julianweise.master.ADBMaster;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributor;
import de.hpi.julianweise.master.data_loading.distribution.ADBDataDistributorFactory;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBLoadAndDistributeDataProcess extends AbstractBehavior<ADBLoadAndDistributeDataProcess.Command> {

    private final ActorRef<CSVParsingActor.Command> csvParser;
    private final Map<ActorRef<ADBPartitionManager.Command>, ActorRef<ADBDataDistributor.Command>> dataDistributors;
    private List<ActorRef<ADBPartitionManager.Command>> partitionManagers = new ObjectArrayList<>();
    private int nextDistributorIndex = 0;
    private final ActorRef<CSVParsingActor.Response> csvResponseWrapper;
    private final ActorRef<ADBDataDistributor.Response> dataDistributorWrapper;
    private final ActorRef<ADBCSVToEntityConverter.Response> converterWrapper;
    private final ActorRef<ADBCSVToEntityConverter.Command> entityConverter;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final AtomicInteger finalizedConverter = new AtomicInteger(0);
    private final AtomicInteger finalizedDistributor = new AtomicInteger(0);
    private ActorRef<ADBMaster.Command> client;
    private final int minNumberOfNodes;


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
    public static class WrappedReceptionistListing implements Command {
        private final Receptionist.Listing listing;
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
        this.entityConverter = this.spawnConverterPool(context);
        this.csvResponseWrapper = this.getCSVResponseWrapper();
        this.dataDistributorWrapper = this.getDataDistributorResponseWrapper();
        this.converterWrapper = this.getConverterWrapper();
        this.minNumberOfNodes = getContext().getSystem().settings().config().getInt("akka.cluster.min-nr-of-members") - 1;
        this.dataDistributors = new Object2ObjectOpenHashMap<>(this.minNumberOfNodes);

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
                .onMessage(WrappedReceptionistListing.class, this::handleWrappedReceptionistListing)
                .build();
    }

    private Behavior<Command> handleWrappedReceptionistListing(WrappedReceptionistListing command) {
        if (command.listing.getServiceInstances(ADBPartitionManager.SERVICE_KEY).size() < this.minNumberOfNodes) {
            return Behaviors.same();
        }
        this.partitionManagers = new ObjectArrayList<>(command.listing.getAllServiceInstances(ADBPartitionManager.SERVICE_KEY));
        command.listing.getServiceInstances(ADBPartitionManager.SERVICE_KEY).forEach(this::createDistributorFor);
        this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.csvResponseWrapper));
        return Behaviors.same();
    }

    private void createDistributorFor(ActorRef<ADBLargeMessageActor.Command> manager) {
        if (this.dataDistributors.containsKey(manager)) {
            return;
        }
        String distributorName = "distributor-" + this.dataDistributors.size();
        val distributor = getContext().spawn(ADBDataDistributorFactory.createDefault(manager), distributorName);
        this.dataDistributors.putIfAbsent(manager, distributor);
    }

    private Behavior<Command> handleStart(Start command) {
        this.getContext().getLog().info("### Start distribution of data to all nodes ###");
        this.client = command.getRespondTo();
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
        if (!chunk.isFinalChunk()) this.csvParser.tell(new CSVParsingActor.ParseNextCSVChunk(this.csvResponseWrapper));
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
            return this.handleConvertedBatch((ADBCSVToEntityConverter.ConvertedBatch) cmd.response);
        } else if (cmd.response instanceof ADBCSVToEntityConverter.Finalized) {
            return this.handleConverterFinalized();
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleConvertedBatch(ADBCSVToEntityConverter.ConvertedBatch response) {
        ObjectList<ADBEntity> entities = response.entities;
        this.getContext().getLog().info("Distribution {} entities to partition node{}", entities.size(), this.nextDistributorIndex);
        this.getNextDistributor().tell(new ADBDataDistributor.DistributeBatch(dataDistributorWrapper, entities));
        return Behaviors.same();
    }

    private Behavior<Command> handleConverterFinalized() {
        if (this.finalizedConverter.incrementAndGet() < this.settings.NUMBER_ENTITY_CONVERTER) {
            return Behaviors.same();
        }
        for(ActorRef<ADBDataDistributor.Command> distributor : this.dataDistributors.values()) {
            distributor.tell(new ADBDataDistributor.ConcludeDistribution(this.dataDistributorWrapper));
        }
        return Behaviors.same();
    }

    private ActorRef<ADBDataDistributor.Command> getNextDistributor() {
        val distributor = this.dataDistributors.get(this.partitionManagers.get(this.nextDistributorIndex));
        this.nextDistributorIndex = (this.nextDistributorIndex + 1) % this.partitionManagers.size();
        return distributor;
    }

    private Behavior<Command> handleDataDistributorResponse(WrappedNodeDistributorResponse response) {
        if (response.getResponse() instanceof ADBDataDistributor.BatchDistributed) {
            return this.handleBatchDistributed();
        }
        else if (response.getResponse() instanceof ADBDataDistributor.Finalized){
            return this.handleDataFullyDistributed();
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleBatchDistributed() {
        return Behaviors.same();
    }

    private Behavior<Command> handleDataFullyDistributed() {
        if (this.finalizedDistributor.incrementAndGet() < this.partitionManagers.size()) {
            return Behaviors.same();
        }
        this.getContext().getLog().info("### Data have been fully loaded into the database ###");
        this.client.tell(new ADBMaster.StartOperationalService());
        System.gc();
        return Behaviors.stopped();
    }

}
