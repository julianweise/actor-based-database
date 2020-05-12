package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query.join.ADBMasterJoinSession;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.node.ADBJoinWithNodeSession;
import de.hpi.julianweise.slave.query.join.node.ADBJoinWithNodeSessionFactory;
import de.hpi.julianweise.slave.query.join.node.ADBJoinWithNodeSessionHandler;
import de.hpi.julianweise.slave.query.join.node.ADBJoinWithNodeSessionHandlerFactory;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.List;
import java.util.Set;

public class ADBSlaveJoinSession extends ADBSlaveQuerySession {

    private final Set<ActorRef<ADBJoinWithNodeSession.Command>> activeJoinSessions = new ObjectOpenHashSet<>();
    private final Set<ActorRef<ADBJoinWithNodeSessionHandler.Command>> activeJoinSessionHandlers = new ObjectOpenHashSet<>();

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class JoinWithShard implements ADBSlaveQuerySession.Command, CborSerializable {
        private ActorRef<ADBSlaveQuerySession.Command> counterpart;
        private int foreignNodeId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class NoMoreShardsToJoinWith implements ADBSlaveQuerySession.Command, CborSerializable {
        private int transactionId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class OpenInterShardJoinSession implements ADBSlaveQuerySession.Command, CborSerializable {
        private ActorRef<ADBJoinWithNodeSession.Command> initiatingSession;
        private int initializingPartitionId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class HandleJoinShardResults implements Command, KryoSerializable {
        private List<ADBKeyPair> joinCandidates;
    }

    @AllArgsConstructor
    public static class FinalizedInterNodeJoin implements Command {}

    @AllArgsConstructor
    public static class InterNodeSessionTerminated implements Command {
        ActorRef<ADBJoinWithNodeSession.Command> session;
    }

    @AllArgsConstructor
    public static class InterNodeSessionHandlerTerminated implements Command {
        ActorRef<ADBJoinWithNodeSessionHandler.Command> sessionHandler;
    }

    @AllArgsConstructor
    public static class RequestNextPartition implements Command {}

    @AllArgsConstructor
    public static class Conclude implements Command {}

    public ADBSlaveJoinSession(ActorContext<Command> context,
                               ActorRef<ADBMasterQuerySession.Command> client,
                               ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientLargeMessageReceiver,
                               int transactionId,
                               ADBJoinQuery query) {
        super(context, client, clientLargeMessageReceiver, transactionId, query);
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(Execute.class, this::handleExecute)
                   .onMessage(JoinWithShard.class, this::handleJoinWithShard)
                   .onMessage(OpenInterShardJoinSession.class, this::handleOpenInterShardJoinSession)
                   .onMessage(HandleJoinShardResults.class, this::handleJoinWithShardResults)
                   .onMessage(NoMoreShardsToJoinWith.class, this::handleNoMoreShardToJoinWith)
                   .onMessage(FinalizedInterNodeJoin.class, this::handleFinalizedInterNodeJoin)
                   .onMessage(InterNodeSessionTerminated.class, this::handleInterNodeSessionTerminated)
                   .onMessage(InterNodeSessionHandlerTerminated.class, this::handleInterNodeSessionHandlerTerminated)
                   .onMessage(RequestNextPartition.class, this::handleRequestNextPartition)
                   .onMessage(Conclude.class, this::handleConclude)
                   .build();
    }

    private Behavior<Command> handleExecute(Execute command) {
        this.getContext().getSelf().tell(new JoinWithShard(this.getContext().getSelf(), ADBSlave.ID));
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithShard(JoinWithShard message) {
        this.getContext().getLog().info("Received request to join with " + message.getForeignNodeId());
        ADBJoinQuery query = (ADBJoinQuery) this.query;
        String sessionName = ADBJoinWithNodeSessionFactory.sessionName(this.transactionId, message.getForeignNodeId());
        val behavior = ADBJoinWithNodeSessionFactory.createDefault(query, this.getContext().getSelf(), message.foreignNodeId);
        ActorRef<ADBJoinWithNodeSession.Command> session = this.getContext().spawn(behavior, sessionName);
        this.getContext().watchWith(session, new InterNodeSessionTerminated(session));
        this.activeJoinSessions.add(session);
        message.getCounterpart().tell(new OpenInterShardJoinSession(session, ADBSlave.ID));
        return Behaviors.same();
    }

    private Behavior<Command> handleOpenInterShardJoinSession(OpenInterShardJoinSession command) {
        String name = ADBJoinWithNodeSessionHandlerFactory.name(this.transactionId, command.initializingPartitionId);
        val behavior = ADBJoinWithNodeSessionHandlerFactory.createDefault(command.getInitiatingSession(),
                this.query, command.getInitializingPartitionId());
        ActorRef<ADBJoinWithNodeSessionHandler.Command> handler = this.getContext().spawn(behavior, name);
        this.getContext().watchWith(handler, new InterNodeSessionHandlerTerminated(handler));
        this.activeJoinSessionHandlers.add(handler);
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithShardResults(HandleJoinShardResults command) {
        this.sendToSession(ADBMasterJoinSession.JoinQueryResults
                .builder()
                .transactionId(this.transactionId)
                .nodeId(ADBSlave.ID)
                .joinResults(command.getJoinCandidates())
                .build());
        return Behaviors.same();
    }


    private Behavior<Command> handleNoMoreShardToJoinWith(NoMoreShardsToJoinWith command) {
        this.getContext().getSelf().tell(new Conclude());
        this.getContext().getLog().info("No more shards to join with this shard for TX #" + command.getTransactionId());
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestNextPartition(RequestNextPartition command) {
        this.getContext().getLog().info("Asking master for next node to join with.");
        this.session.tell(new ADBMasterJoinSession.RequestNextNodeToJoin(this.getContext().getSelf()));
        return Behaviors.same();
    }

    private Behavior<Command> handleFinalizedInterNodeJoin(FinalizedInterNodeJoin command) {
        return Behaviors.same();
    }

    private Behavior<Command> handleInterNodeSessionTerminated(InterNodeSessionTerminated command) {
        this.activeJoinSessions.remove(command.session);
        return Behaviors.same();
    }

    private Behavior<Command> handleInterNodeSessionHandlerTerminated(InterNodeSessionHandlerTerminated command) {
        this.activeJoinSessionHandlers.remove(command.sessionHandler);
        return Behaviors.same();
    }

    private Behavior<Command> handleConclude(Conclude command) {
        if (this.activeJoinSessionHandlers.size() < 1 && this.activeJoinSessions.size() < 1 && this.openTransferSessions.get() < 1) {
            this.concludeTransaction();
            return Behaviors.same();
        }
        this.getContext().scheduleOnce(Duration.ofMillis(50), this.getContext().getSelf(), command);
        if (this.activeJoinSessions.size() > 0) {
            this.getContext().getLog().info("Unable to conclude - waiting for active inter-node join sessions");
        }
        if (this.activeJoinSessionHandlers.size() > 0) {
            this.getContext().getLog().info("Unable to conclude - waiting for active inter-node join session handlers");
        }
        if (this.openTransferSessions.get() > 0) {
            this.getContext().getLog().info("Unable to conclude - waiting for open transfer sessions");
        }
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Join Query";
    }
}
