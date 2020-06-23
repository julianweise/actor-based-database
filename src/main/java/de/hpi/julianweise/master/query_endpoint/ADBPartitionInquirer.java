package de.hpi.julianweise.master.query_endpoint;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.master.ADBMaster;
import de.hpi.julianweise.master.io.ADBResultWriter;
import de.hpi.julianweise.master.query.ADBMasterQuerySessionFactory;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.utility.list.ObjectArrayListCollector;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;


public class ADBPartitionInquirer extends AbstractBehavior<ADBPartitionInquirer.Command> {

    private final Int2ObjectOpenHashMap<ADBTransactionContext> transactionContext = new Int2ObjectOpenHashMap<>();
    private ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManager = new ObjectArrayList<>();
    private ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers = new ObjectArrayList<>();
    private final AtomicInteger transactionCounter = new AtomicInteger();

    public interface Command extends CborSerializable { }

    public interface Response extends CborSerializable { }

    @AllArgsConstructor
    @Getter
    public static class WrappedListing implements Command {
        private final Receptionist.Listing listing;
    }

    @AllArgsConstructor
    @Getter
    public static class WrappedResultLocation implements Command {
        private final ADBResultWriter.ResultLocation response;
    }

    @AllArgsConstructor
    @Getter
    @Builder
    public static class QueryNodes implements Command {
        private final int requestId;
        private final ADBQuery query;
        private final ActorRef<QueryConclusion> respondTo;
    }

    @AllArgsConstructor
    @Getter
    public static class TransactionResultChunk implements Command {
        private final int transactionId;
        private final Iterable<?> results;
        private final int size;
        private final boolean isLast;
    }

    @AllArgsConstructor
    @Getter
    @Builder
    public static class QueryConclusion implements Response {
        private final int transactionId;
        private final int requestId;
        private final long duration;
        private final int resultsCount;
        private final String resultLocation;
    }

    protected ADBPartitionInquirer(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedListing.class, this::handleReceptionistListing)
                .onMessage(QueryNodes.class, this::handleQueryNodes)
                .onMessage(TransactionResultChunk.class, this::handleTransactionResultChunk)
                .onMessage(WrappedResultLocation.class, this::handleResultLocation)
                .build();
    }

    private Behavior<Command> handleReceptionistListing(WrappedListing wrapper) {
        if (wrapper.listing.isForKey(ADBQueryManager.SERVICE_KEY)) {
            return this.handleQueryManagerListing(wrapper);
        }
        return this.handlePartitionManagerListing(wrapper);
    }

    private Behavior<Command> handleQueryManagerListing(WrappedListing wrapper) {
        this.queryManagers = wrapper.getListing().getServiceInstances(ADBQueryManager.SERVICE_KEY)
                                    .stream().sorted(Comparator.comparingInt(ADBMaster::getGlobalIdFor))
                                    .collect(new ObjectArrayListCollector<>());
        return Behaviors.same();
    }

    private Behavior<Command> handlePartitionManagerListing(WrappedListing wrapper) {
        this.partitionManager = wrapper.getListing().getServiceInstances(ADBPartitionManager.SERVICE_KEY)
                                       .stream().sorted(Comparator.comparingInt(ADBMaster::getGlobalIdFor))
                                       .collect(new ObjectArrayListCollector<>());
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryNodes(QueryNodes command) {
        int transactionID = this.transactionCounter.getAndIncrement();
        val resultWriter = getContext().spawn(ADBResultWriter.create(transactionID), "ResultWriterTX-" + transactionID);
        this.transactionContext.put(transactionID, ADBTransactionContext.builder()
                                                                        .initialRequest(command)
                                                                        .requestId(command.requestId)
                                                                        .transactionId(transactionID)
                                                                        .startTime(System.nanoTime())
                                                                        .resultWriter(resultWriter)
                                                                        .respondTo(command.respondTo)
                                                                        .build());
        this.createNewQuerySession(transactionID, command.getQuery());
        return Behaviors.same();
    }

    private void createNewQuerySession(int transactionID, ADBQuery query) {
        this.getContext().spawn(ADBMasterQuerySessionFactory.create(this.queryManagers,
                this.partitionManager, query, transactionID,
                this.getContext().getSelf()), ADBMasterQuerySessionFactory.sessionName(query, transactionID));
    }

    private Behavior<Command> handleTransactionResultChunk(TransactionResultChunk command) {
        ADBTransactionContext txContext = this.transactionContext.get(command.getTransactionId());
        txContext.getResultsCount().getAndAdd(command.size);
        txContext.resultWriter.tell(new ADBResultWriter.Persist(command.results));
        if (command.isLast()) {
            txContext.setDuration(System.nanoTime() - txContext.startTime);
            val resp = getContext().messageAdapter(ADBResultWriter.ResultLocation.class, WrappedResultLocation::new);
            txContext.resultWriter.tell(new ADBResultWriter.FinalizeAndReturnResultLocation(resp));
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleResultLocation(WrappedResultLocation wrapper) {
        ADBTransactionContext txContext = this.transactionContext.remove(wrapper.response.getTransactionId());
        txContext.respondTo.tell(QueryConclusion.builder()
                                                .duration(txContext.duration)
                                                .requestId(txContext.requestId)
                                                .resultsCount(txContext.getResultsCount().get())
                                                .resultLocation(wrapper.response.getResultLocation())
                                                .transactionId(txContext.transactionId)
                                                .build());
        return Behaviors.same();
    }
}
