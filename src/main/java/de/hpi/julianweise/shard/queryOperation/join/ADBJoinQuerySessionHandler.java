package de.hpi.julianweise.shard.queryOperation.join;

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
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.queryOperation.ADBQuerySessionHandler;
import de.hpi.julianweise.utility.largeMessageTransfer.ADBPair;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ADBJoinQuerySessionHandler extends ADBQuerySessionHandler {

    @NoArgsConstructor
    @AllArgsConstructor
    public static class JoinWithShard implements ADBQuerySessionHandler.Command {
        ActorRef<ADBQuerySessionHandler.Command> counterpart;
        int globalShardIndex;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class NoMoreShardsToJoinWith implements ADBQuerySessionHandler.Command {
        int transactionId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class OpenNewJoinWithShardSession implements ADBQuerySessionHandler.Command {
        ActorRef<ADBJoinWithShardSession.Command> session;
        int shardId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class Terminate implements Command {
        int transactionId;
    }

    private Map<String, ADBSortedEntityAttributes> sortedJoinAttributes;
    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());

    public ADBJoinQuerySessionHandler(ActorContext<Command> context,
                                      ActorRef<ADBShard.Command> shard,
                                      ActorRef<ADBQuerySession.Command> client, int transactionId,
                                      ADBJoinQuery query, final List<ADBEntityType> data, int globalShardId) {
        super(context, shard, client, transactionId, query, data, globalShardId);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, this::handlePostStop)
                .onMessage(OpenNewJoinWithShardSession.class, this::handleOpenNewJoinWithShardSession)
                .onMessage(Execute.class, this::handleExecute)
                .onMessage(JoinWithShard.class, this::handleJoinWithShard)
                .onMessage(ADBJoinWithShardSession.HandleJoinShardsResults.class, this::handleJoinWithShardResults)
                .onMessage(NoMoreShardsToJoinWith.class, this::handleNoMoreShardToJoinWith)
                .onMessage(Terminate.class, this::handleTerminate)
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
        this.getContext().getLog().info("Received master request to join with " + message.globalShardIndex);
        ActorRef<ADBJoinWithShardSession.Command> joinWithShardSession =
                this.getContext().spawn(ADBJoinWithShardSessionFactory.createDefault(this.query,
                        this.sortedJoinAttributes, this.getContext().getSelf()),
                        ADBJoinWithShardSessionFactory.sessionName(this.transactionId, this.globalShardId,
                                message.globalShardIndex));
        message.counterpart.tell(new OpenNewJoinWithShardSession(joinWithShardSession, this.globalShardId));
        return Behaviors.same();
    }

    private Behavior<Command> handleOpenNewJoinWithShardSession(OpenNewJoinWithShardSession command) {
        this.getContext().spawn(ADBJoinWithShardSessionHandlerFactory
                        .createDefault(command.session, this.query, this.sortedJoinAttributes, this.data),
                ADBJoinWithShardSessionHandlerFactory.sessionHandlerName(this.transactionId, this.globalShardId,
                        command.shardId));
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithShardResults(ADBJoinWithShardSession.HandleJoinShardsResults command) {
        this.getContext().getLog().info("Generated " + command.joinCandidates.size() + " join candidates. Sending to " +
                "master ...");
        this.client.tell(new ADBJoinQuerySession.RequestNextShardComparison(this.shard, this.getContext().getSelf()));
        final AtomicInteger counter = new AtomicInteger();
        Collection<List<ADBPair<ADBEntityType, ADBEntityType>>> results = command.joinCandidates
                .stream().map(pair -> new ADBPair<>(this.data.get(pair.getKey()), pair.getValue()))
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / (this.settings.QUERY_RESPONSE_CHUNK_SIZE / 3)))
                .values();

        results.forEach(chunk -> this.client.tell(ADBJoinQuerySession.JoinQueryResults
                .builder()
                .transactionId(this.transactionId)
                .globalShardId(this.globalShardId)
                .joinResults(chunk)
                .build()));
        return Behaviors.same();
    }

    private Behavior<Command> handleNoMoreShardToJoinWith(NoMoreShardsToJoinWith command) {
        this.concludeTransaction();
        return Behaviors.same();
    }

    private Behavior<Command> handleTerminate(Terminate command) {
        this.getContext().getLog().info("Going to shut down JoinQuery Session for transaction #"
                + command.transactionId);
        return Behaviors.stopped();
    }

    @Override
    protected String getQuerySessionName() {
        return "Join Query";
    }
}
