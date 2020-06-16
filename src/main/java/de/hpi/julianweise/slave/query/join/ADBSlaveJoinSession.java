package de.hpi.julianweise.slave.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query.join.ADBMasterJoinSession;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.node.ADBJoinNodesContext;
import de.hpi.julianweise.slave.query.join.node.ADBJoinWithNodeSession;
import de.hpi.julianweise.slave.query.join.node.ADBJoinWithNodeSessionFactory;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.Set;

public class ADBSlaveJoinSession extends ADBSlaveQuerySession {

    private final Set<ActorRef<ADBJoinWithNodeSession.Command>> activeJoinSessions = new ObjectOpenHashSet<>();

    @NoArgsConstructor
    @Getter
    @AllArgsConstructor
    public static class JoinWithNode implements ADBSlaveQuerySession.Command, CborSerializable {
        private ADBJoinNodesContext context;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class NoMoreNodesToJoinWith implements ADBSlaveQuerySession.Command, CborSerializable {
        private int transactionId;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class JoinPartitionsResults implements Command, KryoSerializable {
        private ADBPartialJoinResult joinCandidates;
    }

    @AllArgsConstructor
    public static class InterNodeSessionTerminated implements Command {
        ActorRef<ADBJoinWithNodeSession.Command> session;
    }

    @AllArgsConstructor
    public static class RequestNextPartitions implements Command {
    }

    @AllArgsConstructor
    public static class Conclude implements Command {
    }

    public ADBSlaveJoinSession(ActorContext<Command> context,
                               ActorRef<ADBMasterQuerySession.Command> client,
                               ADBJoinQueryContext joinQueryContext) {
        super(context, client, joinQueryContext);
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(Execute.class, this::handleExecute)
                   .onMessage(JoinWithNode.class, this::handleJoinWithNode)
                   .onMessage(JoinPartitionsResults.class, this::handleJoinWithNodeResults)
                   .onMessage(NoMoreNodesToJoinWith.class, this::handleNoMoreNodeToJoinWith)
                   .onMessage(InterNodeSessionTerminated.class, this::handleInterNodeSessionTerminated)
                   .onMessage(RequestNextPartitions.class, this::handleRequestNextPartition)
                   .onMessage(Conclude.class, this::handleConclude)
                   .build();
    }

    private Behavior<Command> handleExecute(Execute command) {
        this.getContext().getSelf().tell(new JoinWithNode(ADBJoinNodesContext.builder()
                                                                             .left(ADBPartitionManager.getInstance())
                                                                             .leftNodeId(ADBSlave.ID)
                                                                             .right(ADBPartitionManager.getInstance())
                                                                             .rightNodeId(ADBSlave.ID)
                                                                             .build()));
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithNode(JoinWithNode message) {
        ADBJoinNodesContext context = message.getContext();
        this.getContext().getLog().info("Requested to execute "  + context.toString());
        String sessionName = ADBJoinWithNodeSessionFactory.sessionName(queryContext.getTransactionId(), context);
        val behavior = ADBJoinWithNodeSessionFactory.createDefault((ADBJoinQueryContext) this.queryContext,
                this.getContext().getSelf(), context);
        ActorRef<ADBJoinWithNodeSession.Command> session = this.getContext().spawn(behavior, sessionName);
        this.getContext().watchWith(session, new InterNodeSessionTerminated(session));
        this.activeJoinSessions.add(session);
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithNodeResults(JoinPartitionsResults command) {
        this.sendToSession(ADBMasterJoinSession.JoinQueryResults
                .builder()
                .transactionId(queryContext.getTransactionId())
                .nodeId(ADBSlave.ID)
                .joinResults(command.getJoinCandidates())
                .build());
        return Behaviors.same();
    }


    private Behavior<Command> handleNoMoreNodeToJoinWith(NoMoreNodesToJoinWith command) {
        this.getContext().getSelf().tell(new Conclude());
        this.getContext().getLog().info("No more nodes to join with this node for TX #" + command.getTransactionId());
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestNextPartition(RequestNextPartitions command) {
        this.getContext().getLog().info("Asking master for next node to join with.");
        this.session.tell(new ADBMasterJoinSession.RequestNextNodeToJoin(this.getContext().getSelf()));
        return Behaviors.same();
    }

    private Behavior<Command> handleInterNodeSessionTerminated(InterNodeSessionTerminated command) {
        this.activeJoinSessions.remove(command.session);
        return Behaviors.same();
    }

    private Behavior<Command> handleConclude(Conclude command) {
        if (this.activeJoinSessions.size() < 1 && this.openTransferSessions.get() < 1) {
            this.concludeTransaction();
            return Behaviors.same();
        }
        this.getContext().scheduleOnce(Duration.ofMillis(50), this.getContext().getSelf(), command);
        if (this.activeJoinSessions.size() > 0) {
            this.getContext().getLog().info("Unable to conclude - waiting for active inter-node join sessions");
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
