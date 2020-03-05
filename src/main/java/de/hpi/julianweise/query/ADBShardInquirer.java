package de.hpi.julianweise.query;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
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
        private ADBSelectionQuery query;
        private ActorRef<Response> respondTo;
    }

    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    public static class ConcludeTransaction implements Command {
        private int transactionId;
    }

    @AllArgsConstructor
    @Getter
    public static class QueryResults implements Command {
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
    private final Map<Integer, ActorRef<Response>> transactions = new HashMap<>();
    private final Map<Integer, List<ADBEntityType>> queryResults = new HashMap<>();
    private final Map<Integer, Integer> requestTransactionMapping = new HashMap<>();
    private final AtomicInteger transactionCounter = new AtomicInteger();

    protected ADBShardInquirer(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(WrappedListing.class, this::handleReceptionistListing)
                .onMessage(QueryShards.class, this::handleQueryShards)
                .onMessage(QueryResults.class, this::handleQueryResults)
                .onMessage(ConcludeTransaction.class, this::handleConcludeTransaction)
                .build();
    }

    private Behavior<Command> handleReceptionistListing(WrappedListing wrapper) {
        this.shards.addAll(wrapper.getListing().getServiceInstances(ADBShard.SERVICE_KEY));
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryShards(QueryShards command) {
        int transactionID = this.transactionCounter.getAndIncrement();
        this.requestTransactionMapping.put(transactionID, command.getRequestId());
        this.transactions.put(transactionID, command.getRespondTo());
        this.queryResults.put(transactionID, new ArrayList<>());
        this.shards.forEach(shard -> shard.tell(ADBShard.QueryEntities.builder()
                                                                      .transactionId(transactionID)
                                                                      .query(command.getQuery())
                                                                      .respondTo(this.getContext().getSelf())
                                                                      .build()));
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryResults(QueryResults response) {
        if (this.transactionIsInactive(response.getTransactionId())) {
            return Behaviors.same();
        }
        this.queryResults.get(response.getTransactionId()).addAll(response.getResults());
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeTransaction(ConcludeTransaction response) {
        if (this.transactionIsInactive(response.getTransactionId())) {
            return Behaviors.same();
        }
        ActorRef<Response> client = this.transactions.get(response.transactionId);

        this.transactions.remove(response.transactionId);

        client.tell(new AllQueryResults(this.requestTransactionMapping.get(response.getTransactionId()),
                this.queryResults.get(response.getTransactionId())));
        this.queryResults.remove(response.getTransactionId());
        return Behaviors.same();
    }

    private boolean transactionIsInactive(int transactionId) {
        return !this.transactions.containsKey(transactionId) || !this.queryResults.containsKey(transactionId);
    }
}
