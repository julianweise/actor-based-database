package de.hpi.julianweise.query.session.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandler;
import de.hpi.julianweise.shard.query_operation.join.ADBJoinQuerySessionHandler;
import de.hpi.julianweise.utility.CborSerializable;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBJoinQuerySession extends ADBQuerySession {

    private final JoinDistributionPlan distributionPlan;
    private final Set<ActorRef<ADBJoinQuerySessionHandler.Command>> completedSessions;
    private final List<ADBPair<ADBEntityType, ADBEntityType>> queryResults = new ArrayList<>();
    private final AtomicInteger expectedPartialResults = new AtomicInteger();

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class RequestNextShardComparison implements ADBQuerySession.Command, CborSerializable {
        private ActorRef<ADBShard.Command> requestingShard;
        private ActorRef<ADBQuerySessionHandler.Command> respondTo;

    }
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TriggerNextShardComparison implements ADBQuerySession.Command, CborSerializable {
        private ActorRef<ADBShard.Command> requestingShard;
        private ActorRef<ADBShard.Command> nextJoiningShard;
        private ActorRef<ADBQuerySessionHandler.Command> respondTo;

    }
    @NoArgsConstructor
    @AllArgsConstructor
    @SuperBuilder
    @Getter
    public static class JoinQueryResults extends ADBQuerySession.QueryResults {
        private List<ADBPair<ADBEntityType, ADBEntityType>> joinResults;
    }

    public ADBJoinQuerySession(ActorContext<ADBQuerySession.Command> context,
                               List<ActorRef<ADBShard.Command>> shards, int transactionId,
                               ActorRef<ADBShardInquirer.Command> parent,
                               ADBJoinQuery query) {
        super(context, shards, transactionId, parent);
        this.distributionPlan = new JoinDistributionPlan(shards, context.getLog());
        this.completedSessions = new HashSet<>();
        // Send initial query
        this.shards.forEach(shard -> shard.tell(ADBShard.QueryEntities.builder()
                                                                      .transactionId(transactionId)
                                                                      .query(query)
                                                                      .clientLargeMessageReceiver(this.initializeTransferWrapper)
                                                                      .respondTo(this.getContext().getSelf())
                                                                      .build()));
    }

    @Override
    public Receive<ADBQuerySession.Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(RequestNextShardComparison.class, this::handleRequestNextShardComparison)
                   .onMessage(JoinQueryResults.class, this::handleJoinQueryResults)
                   .onMessage(ConcludeTransaction.class, this::handleConcludeTransaction)
                   .onMessage(TriggerNextShardComparison.class, this::handleTriggerNextShardComparison)
                   .build();
    }

    private Behavior<ADBQuerySession.Command> handleRequestNextShardComparison(RequestNextShardComparison command) {
        ActorRef<ADBShard.Command> nextJoinShard = this.distributionPlan.getNextJoinShardFor(command.requestingShard);

        if (nextJoinShard == null) {
            this.getContext().getLog().info("No sufficient next join partner for shard #"
                    + this.shards.indexOf(command.requestingShard));
            command.respondTo.tell(new ADBJoinQuerySessionHandler.NoMoreShardsToJoinWith(this.transactionId));
            return Behaviors.same();
        }

        this.getContext().getSelf().tell(
                new TriggerNextShardComparison(command.requestingShard, nextJoinShard, command.respondTo));
        return Behaviors.same();
    }

    private Behavior<ADBQuerySession.Command> handleTriggerNextShardComparison(TriggerNextShardComparison command) {
        if (this.shardToSessionMapping.containsKey(command.nextJoiningShard)) {
            this.expectedPartialResults.incrementAndGet();
            this.getContext().getLog().info(String.format("Asking shard #%d to join with shard #%d",
                    this.shards.indexOf(command.requestingShard), this.shards.indexOf(command.nextJoiningShard)));
            command.respondTo.tell(new ADBJoinQuerySessionHandler.JoinWithShard(
                    this.shardToSessionMapping.get(command.nextJoiningShard), this.shards.indexOf(command.nextJoiningShard)));
        } else {
            this.getContext().getLog().warn("No Shard-to-Session mapping present for " + command.nextJoiningShard);
            this.getContext().scheduleOnce(Duration.ofSeconds(1), this.getContext().getSelf(), command);
        }
        return Behaviors.same();
    }

    private Behavior<ADBQuerySession.Command> handleJoinQueryResults(JoinQueryResults results) {
        this.getContext().getLog().info("Received " + results.joinResults.size() + " join result tuples from shard#"
                + results.getGlobalShardId() + " for transaction #" + results.getTransactionId());
        this.queryResults.addAll(results.joinResults);
        return Behaviors.same();
    }

    private Behavior<ADBQuerySession.Command> handleConcludeTransaction(ConcludeTransaction command) {
        this.completedSessions.add(this.shardToSessionMapping.get(command.getShard()));
        if (this.shards.size() == this.completedSessions.size()) {
            this.completedSessions.forEach(sessionHandler ->
                    sessionHandler.tell(new ADBJoinQuerySessionHandler.Terminate(this.transactionId)));
            this.parent.tell(new ADBShardInquirer.TransactionResults(this.transactionId, this.queryResults.toArray()));
            return this.concludeTransaction();
        }
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Join Query";
    }
}
