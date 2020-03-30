package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.Signal;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.join.ADBJoinQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.ADBQuerySessionHandler;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ADBJoinQuerySessionHandler extends ADBQuerySessionHandler {

    private Map<String, ADBSortedEntityAttributes> sortedJoinAttributes;

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class JoinWithShard implements ADBQuerySessionHandler.Command {
        private ActorRef<ADBQuerySessionHandler.Command> counterpart;
        private int globalShardIndex;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class NoMoreShardsToJoinWith implements ADBQuerySessionHandler.Command {
        private int transactionId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class OpenNewJoinWithShardSession implements ADBQuerySessionHandler.Command {
        private ActorRef<ADBJoinWithShardSession.Command> session;
        private int shardId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class HandleJoinShardResults implements Command {
        private Set<ADBPair<Integer, ADBEntityType>> joinCandidates;
    }

    public ADBJoinQuerySessionHandler(ActorContext<Command> context,
                                      ActorRef<ADBShard.Command> shard,
                                      ActorRef<ADBQuerySession.Command> client,
                                      ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientLargeMessageReceiver,
                                      int transactionId,
                                      ADBJoinQuery query,
                                      final List<ADBEntityType> data,
                                      int globalShardId) {
        super(context, shard, client, clientLargeMessageReceiver, transactionId, query, data, globalShardId);
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                .onSignal(PostStop.class, this::handlePostStop)
                .onMessage(OpenNewJoinWithShardSession.class, this::handleOpenNewJoinWithShardSession)
                .onMessage(Execute.class, this::handleExecute)
                .onMessage(JoinWithShard.class, this::handleJoinWithShard)
                .onMessage(HandleJoinShardResults.class, this::handleJoinWithShardResults)
                .onMessage(NoMoreShardsToJoinWith.class, this::handleNoMoreShardToJoinWith)
                .build();
    }

    private Behavior<Command> handlePostStop(Signal postStop) {
        this.sortedJoinAttributes = null;
        System.gc();
        return Behaviors.same();
    }

    private Behavior<Command> handleExecute(Execute command) {
        this.sortedJoinAttributes = ADBSortedEntityAttributes.of((ADBJoinQuery) this.query, this.data);
        this.client.tell(new ADBJoinQuerySession.RequestNextShardComparison(this.shard, this.getContext().getSelf()));
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithShard(JoinWithShard message) {
        this.getContext().getLog().info("Received master request to join with " + message.getGlobalShardIndex());
        ActorRef<ADBJoinWithShardSession.Command> joinWithShardSession =
                this.getContext().spawn(ADBJoinWithShardSessionFactory.createDefault(this.query,
                        this.sortedJoinAttributes, this.getContext().getSelf()),
                        ADBJoinWithShardSessionFactory.sessionName(this.transactionId, this.globalShardId,
                                message.getGlobalShardIndex()));
        message.getCounterpart().tell(new OpenNewJoinWithShardSession(joinWithShardSession, this.globalShardId));
        return Behaviors.same();
    }

    private Behavior<Command> handleOpenNewJoinWithShardSession(OpenNewJoinWithShardSession command) {
        this.getContext().spawn(ADBJoinWithShardSessionHandlerFactory
                        .createDefault(command.session, this.query, this.sortedJoinAttributes, this.data),
                ADBJoinWithShardSessionHandlerFactory.sessionHandlerName(this.transactionId, this.globalShardId,
                        command.shardId));
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithShardResults(HandleJoinShardResults command) {
        this.getContext().getLog().info("Generated " + command.getJoinCandidates().size() + " join candidates. " +
                "Sending to master ...");
        this.client.tell(new ADBJoinQuerySession.RequestNextShardComparison(this.shard, this.getContext().getSelf()));
        List<ADBPair<ADBEntityType, ADBEntityType>> results = command
                .getJoinCandidates()
                .stream()
                .map(pair -> new ADBPair<>(this.data.get(pair.getKey()), pair.getValue()))
                .collect(Collectors.toList());

        this.sendToSession(ADBJoinQuerySession.JoinQueryResults
                .builder()
                .transactionId(this.transactionId)
                .globalShardId(this.globalShardId)
                .joinResults(results)
                .build(), results.size());

        return Behaviors.same();
    }

    private Behavior<Command> handleNoMoreShardToJoinWith(NoMoreShardsToJoinWith command) {
        this.concludeTransaction();
        this.getContext().getLog().info("No more shards to join with this shard for TX #" + command.getTransactionId());
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Join Query";
    }
}
