package de.hpi.julianweise.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.query.session.ADBQuerySessionFactory;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.agrona.collections.Int2IntHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


public class ADBShardInquirer extends AbstractBehavior<ADBShardInquirer.Command> {

    private final Set<ActorRef<ADBShard.Command>> shards = new HashSet<>();
    private final Int2IntHashMap transactionRequestMapping = new Int2IntHashMap(-1);
    private final Map<Integer, ActorRef<ADBShardInquirer.Response>> requestClientMapping = new HashMap<>();
    private final AtomicInteger transactionCounter = new AtomicInteger();

    public interface Command extends CborSerializable {

    }
    public interface Response extends CborSerializable {

    }
    @AllArgsConstructor
    @Getter
    public static class WrappedListing implements Command {
        private Receptionist.Listing listing;

    }
    @AllArgsConstructor
    @Getter
    @Builder
    public static class QueryShards implements Command {
        private int requestId;
        private ADBQuery query;
        private ActorRef<Response> respondTo;

    }
    @AllArgsConstructor
    @Getter
    public static class TransactionResults implements Command {
        private int transactionId;
        private Object[] results;

    }
    @AllArgsConstructor
    @Getter
    public static class AllQueryResults implements Response {
        private int requestId;
        private Object[] results;

    }

    protected ADBShardInquirer(ActorContext<Command> context) {
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
        this.shards.addAll(wrapper.getListing().getServiceInstances(ADBShard.SERVICE_KEY));
        List<ActorRef<ADBShard.Command>> numberedShards = new ArrayList<>(this.shards);
        for(int i = 0; i < numberedShards.size(); i ++) {
            this.getContext().getLog().info("Shard " + numberedShards.get(i) + " has globalID " + i);
        }
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
        this.getContext().spawn(ADBQuerySessionFactory.create(new ArrayList<>(this.shards), query, transactionID,
                this.getContext().getSelf()), ADBQuerySessionFactory.sessionName(query, transactionID));
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
