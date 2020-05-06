package de.hpi.julianweise.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.master.query.ADBMasterQuerySessionFactory;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import it.unimi.dsi.fastutil.objects.ObjectArraySet;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.agrona.collections.Int2IntHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


public class ADBPartitionInquirer extends AbstractBehavior<ADBPartitionInquirer.Command> {

    private final Set<ActorRef<ADBQueryManager.Command>> queryManagers = new ObjectArraySet<>();
    private final Int2IntHashMap transactionRequestMapping = new Int2IntHashMap(-1);
    private final Map<Integer, ActorRef<ADBPartitionInquirer.Response>> requestClientMapping = new HashMap<>();
    private final AtomicInteger transactionCounter = new AtomicInteger();

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
        private final ActorRef<Response> respondTo;

    }
    @AllArgsConstructor
    @Getter
    public static class TransactionResults implements Command {
        private final int transactionId;
        private final Object[] results;

    }
    @AllArgsConstructor
    @Getter
    public static class AllQueryResults implements Response {
        private final int requestId;
        private final Object[] results;

    }

    protected ADBPartitionInquirer(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedListing.class, this::handleReceptionistListing)
                .onMessage(QueryShards.class, this::handleQueryShards)
                .onMessage(TransactionResults.class, this::handleTransactionResults)
                .build();
    }

    private Behavior<Command> handleReceptionistListing(WrappedListing wrapper) {
        this.queryManagers.addAll(wrapper.getListing().getServiceInstances(ADBQueryManager.SERVICE_KEY));
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryShards(QueryShards command) {
        int transactionID = this.transactionCounter.getAndIncrement();
        this.transactionRequestMapping.put(transactionID, command.getRequestId());
        this.requestClientMapping.put(command.getRequestId(), command.respondTo);

        this.createNewQuerySession(transactionID, command.getQuery());
        return Behaviors.same();
    }

    private void createNewQuerySession(int transactionID, ADBQuery query) {
        this.getContext().spawn(ADBMasterQuerySessionFactory.create(new ArrayList<>(this.queryManagers), query, transactionID,
                this.getContext().getSelf()), ADBMasterQuerySessionFactory.sessionName(query, transactionID));
    }

    private Behavior<Command> handleTransactionResults(TransactionResults command) {
        int requestId = this.transactionRequestMapping.get(command.getTransactionId());
        ActorRef<Response> client = this.requestClientMapping.get(requestId);
        client.tell(new AllQueryResults(requestId, command.getResults()));

        this.transactionRequestMapping.remove(command.getTransactionId());
        this.requestClientMapping.remove(requestId);
        return Behaviors.same();
    }
}
