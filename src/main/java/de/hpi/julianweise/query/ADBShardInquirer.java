package de.hpi.julianweise.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.ADBQuerySessionFactory;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


public class ADBShardInquirer extends AbstractBehavior<ADBShardInquirer.Command> {

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
        private List<ADBEntityType> results;
    }

    @AllArgsConstructor
    @Getter
    public static class AllQueryResults implements Response {
        private int requestId;
        private List<ADBEntityType> results;
    }

    private final Set<ActorRef<ADBShard.Command>> shards = new HashSet<>();
    private final Map<Integer, Integer> transactionRequestMapping = new HashMap<>();
    private final Map<Integer, ActorRef<ADBQuerySession.Command>> transactionSessions = new HashMap<>();
    private final Map<Integer, ActorRef<ADBShardInquirer.Response>> requestClientMapping = new HashMap<>();
    private final AtomicInteger transactionCounter = new AtomicInteger();

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
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryShards(QueryShards command) {
        int transactionID = this.transactionCounter.getAndIncrement();
        this.transactionRequestMapping.put(transactionID, command.getRequestId());
        this.requestClientMapping.put(command.getRequestId(), command.respondTo);

        this.transactionSessions.put(transactionID, this.createNewQuerySession(transactionID, command.getQuery()));
        return Behaviors.same();
    }

    private ActorRef<ADBQuerySession.Command> createNewQuerySession(int transactionID, ADBQuery query) {
        return this.getContext().spawn(ADBQuerySessionFactory.create(new HashSet<>(this.shards), query, transactionID,
                this.getContext().getSelf()), "ADBQuerySession_" + transactionID);
    }

    private Behavior<Command> handleTransactionResults(TransactionResults command) {
        int requestId = this.transactionRequestMapping.get(command.getTransactionId());
        ActorRef<Response> client = this.requestClientMapping.get(requestId);
        client.tell(new AllQueryResults(requestId, command.getResults()));

        this.transactionSessions.remove(command.getTransactionId());
        this.transactionRequestMapping.remove(command.getTransactionId());
        this.requestClientMapping.remove(requestId);
        return Behaviors.same();
    }
}
