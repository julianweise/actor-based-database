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

    private Map<String, ADBSortedEntityAttributes> sortedAttributes;

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
    public static class OpenInterShardJoinSession implements ADBQuerySessionHandler.Command {
        private ActorRef<ADBJoinWithShardSession.Command> initiatingSession;
        private int initiatingShardId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class HandleJoinShardResults implements Command {
        private List<ADBPair<Integer, ADBEntityType>> joinCandidates;
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
                .onMessage(OpenInterShardJoinSession.class, this::handleOpenInterShardJoinSession)
                .onMessage(Execute.class, this::handleExecute)
                .onMessage(JoinWithShard.class, this::handleJoinWithShard)
                .onMessage(HandleJoinShardResults.class, this::handleJoinWithShardResults)
                .onMessage(NoMoreShardsToJoinWith.class, this::handleNoMoreShardToJoinWith)
                .build();
    }

    private Behavior<Command> handlePostStop(Signal postStop) {
        this.sortedAttributes = null;
        System.gc();
        return Behaviors.same();
    }

    private Behavior<Command> handleExecute(Execute command) {
        this.sortedAttributes = ADBSortedEntityAttributes.of((ADBJoinQuery) this.query, this.data);
        this.getContext().getSelf().tell(new JoinWithShard(this.getContext().getSelf(), this.globalShardId));
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithShard(JoinWithShard message) {
        this.getContext().getLog().info("Received master request to join with " + message.getGlobalShardIndex());
        ActorRef<ADBJoinWithShardSession.Command> joinWithShardSession =
                this.getContext().spawn(ADBJoinWithShardSessionFactory.createDefault((ADBJoinQuery) this.query,
                        this.sortedAttributes, this.getContext().getSelf(), this.globalShardId, message.globalShardIndex),
                        ADBJoinWithShardSessionFactory.sessionName(this.transactionId, this.globalShardId,
                                message.getGlobalShardIndex()));
        message.getCounterpart().tell(new OpenInterShardJoinSession(joinWithShardSession, this.globalShardId));
        return Behaviors.same();
    }

    private Behavior<Command> handleOpenInterShardJoinSession(OpenInterShardJoinSession command) {
        this.getContext().spawn(ADBJoinWithShardSessionHandlerFactory
                        .createDefault(command.getInitiatingSession(), this.query, this.sortedAttributes, this.data,
                                this.globalShardId, command.getInitiatingShardId()),
                ADBJoinWithShardSessionHandlerFactory.sessionHandlerName(this.transactionId, this.globalShardId,
                        command.initiatingShardId));
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithShardResults(HandleJoinShardResults command) {
        this.session.tell(new ADBJoinQuerySession.RequestNextShardComparison(this.shard, this.getContext().getSelf()));
        this.getContext().getLog().info("Sending " + command.getJoinCandidates().size() + " candidates to master.");
        List<ADBPair<ADBEntityType, ADBEntityType>> results = command
                .getJoinCandidates()
                .stream()
                .map(this::materializeResultTuple)
                .collect(Collectors.toList());

        this.sendToSession(ADBJoinQuerySession.JoinQueryResults
                .builder()
                .transactionId(this.transactionId)
                .globalShardId(this.globalShardId)
                .joinResults(results)
                .build(), results.size());

        return Behaviors.same();
    }

    private ADBPair<ADBEntityType, ADBEntityType> materializeResultTuple(ADBPair<Integer, ADBEntityType> resultTuple) {
        if (resultTuple.isFlipped()) {
            return new ADBPair<>(resultTuple.getValue(), this.data.get(resultTuple.getKey()));
        }
        return new ADBPair<>(this.data.get(resultTuple.getKey()), resultTuple.getValue());
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
