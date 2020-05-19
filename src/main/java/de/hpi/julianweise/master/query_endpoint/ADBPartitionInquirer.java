package de.hpi.julianweise.master.query_endpoint;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.master.io.ADBResultWriter;
import de.hpi.julianweise.master.query.ADBMasterQuerySessionFactory;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.agrona.collections.Int2ObjectHashMap;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


public class ADBPartitionInquirer extends AbstractBehavior<ADBPartitionInquirer.Command> {

    @SuppressWarnings("rawtypes")
    private final Int2ObjectHashMap<ObjectList> results = new Int2ObjectHashMap<>();
    private final Set<ActorRef<ADBQueryManager.Command>> queryManagers = new ObjectArraySet<>();
    private final Int2ObjectHashMap<QueryShards> transactionToRequest = new Int2ObjectHashMap<>();
    private final AtomicInteger transactionCounter = new AtomicInteger();
    private final ActorRef<ADBResultWriter.Command> resultWriter;

    public interface Command extends CborSerializable {

    }

    public interface Response extends CborSerializable {

    }

    @AllArgsConstructor
    @Getter
    public static class WrappedListing implements Command {
        private final Receptionist.Listing listing;

    }

    @AllArgsConstructor
    @Getter
    @Builder
    public static class QueryShards implements Command {
        private final int requestId;
        private final ADBQuery query;
        private final boolean async;
        private final ActorRef<Response> respondTo;

    }

    @SuppressWarnings("rawtypes")
    @AllArgsConstructor
    @Getter
    public static class TransactionResultChunk implements Command {
        private final int transactionId;
        private final ObjectList results;
        private final boolean isLast;
    }

    @AllArgsConstructor
    @Getter
    public static class SyncQueryResults implements Response {
        private final int requestId;
        private final Object[] results;
    }

    @AllArgsConstructor
    @Getter
    public static class AsyncQueryResults implements Response {
        private final int requestId;
        private final int transactionId;
    }

    protected ADBPartitionInquirer(ActorContext<Command> context) {
        super(context);
        this.resultWriter = context.spawn(ADBResultWriter.create(), "ResultWriter");
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedListing.class, this::handleReceptionistListing)
                .onMessage(QueryShards.class, this::handleQueryShards)
                .onMessage(TransactionResultChunk.class, this::handleTransactionResultChunk)
                .build();
    }

    private Behavior<Command> handleReceptionistListing(WrappedListing wrapper) {
        this.queryManagers.addAll(wrapper.getListing().getServiceInstances(ADBQueryManager.SERVICE_KEY));
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryShards(QueryShards command) {
        int transactionID = this.transactionCounter.getAndIncrement();
        this.transactionToRequest.put(transactionID, command);

        if (command.async) {
            command.respondTo.tell(new AsyncQueryResults(command.requestId, transactionID));
        }

        this.createNewQuerySession(transactionID, command.getQuery());
        return Behaviors.same();
    }

    private void createNewQuerySession(int transactionID, ADBQuery query) {
        this.getContext().spawn(ADBMasterQuerySessionFactory.create(new ObjectArrayList<>(this.queryManagers), query, transactionID,
                this.getContext().getSelf()), ADBMasterQuerySessionFactory.sessionName(query, transactionID));
    }

    @SuppressWarnings("unchecked")
    private Behavior<Command> handleTransactionResultChunk(TransactionResultChunk command) {
        QueryShards request = this.transactionToRequest.get(command.getTransactionId());
        if (request.async) {
            resultWriter.tell(new ADBResultWriter.Persist(command.transactionId, request.requestId, command.results.toArray()));
        } else {
            if (this.results.containsKey(command.getTransactionId())) {
                this.results.get(command.getTransactionId()).addAll(command.results);
            } else {
                this.results.putIfAbsent(command.getTransactionId(), command.results);
            }
            if (command.isLast) {
                request.respondTo.tell(new SyncQueryResults(request.requestId, results.get(command.getTransactionId()).toArray()));
                this.results.remove(command.getTransactionId());
            }
        }
        return Behaviors.same();
    }
}
