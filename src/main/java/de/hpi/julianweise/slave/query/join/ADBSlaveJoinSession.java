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
import de.hpi.julianweise.slave.query.join.node.ADBNodeJoin;
import de.hpi.julianweise.slave.query.join.node.ADBNodeJoinContext;
import de.hpi.julianweise.slave.query.join.node.ADBNodeJoinFactory;
import de.hpi.julianweise.slave.query.join.node.ADBPartitionJoinTask;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class ADBSlaveJoinSession extends ADBSlaveQuerySession {

    private final ObjectList<ActorRef<ADBNodeJoin.Command>> activeJoinSessions = new ObjectArrayList<>();

    @NoArgsConstructor
    @Getter
    @AllArgsConstructor
    public static class JoinWithNode implements ADBSlaveQuerySession.Command, CborSerializable {
        private ADBNodeJoinContext context;
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
    public static class StealWorkFrom implements Command, CborSerializable {
        private ActorRef<ADBSlaveQuerySession.Command> target;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class JoinPartitionsResults implements Command, KryoSerializable {
        private ADBPartialJoinResult joinCandidates;
    }

    @AllArgsConstructor
    public static class InterNodeSessionTerminated implements Command {
        ActorRef<ADBNodeJoin.Command> session;
    }

    @AllArgsConstructor
    public static class RequestNextPartitions implements Command {
    }

    @AllArgsConstructor
    public static class Conclude implements Command {
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class HandOverWork implements Command, CborSerializable {
        private ActorRef<ADBSlaveQuerySession.Command> respondTo;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class TakeOverWork implements Command, CborSerializable {
        ADBNodeJoinContext context;
        List<ADBPartitionJoinTask> joinTasks;
        boolean last;
    }

    List<ADBPartitionJoinTask> workToTakeOver = new ObjectArrayList<>();

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
                   .onMessage(StealWorkFrom.class, this::handleStealWorkFrom)
                   .onMessage(RequestNextPartitions.class, this::handleRequestNextPartition)
                   .onMessage(HandOverWork.class, this::handleHandOverWork)
                   .onMessage(TakeOverWork.class, this::handleTakeOverWork)
                   .onMessage(Conclude.class, this::handleConclude)
                   .build();
    }

    private Behavior<Command> handleExecute(Execute command) {
        this.getContext().getSelf().tell(new JoinWithNode(ADBNodeJoinContext.builder()
                                                                            .executorNodeId(ADBSlave.ID)
                                                                            .transactionId(this.queryContext.getTransactionId())
                                                                            .left(ADBPartitionManager.getInstance())
                                                                            .leftNodeId(ADBSlave.ID)
                                                                            .right(ADBPartitionManager.getInstance())
                                                                            .rightNodeId(ADBSlave.ID)
                                                                            .build()));
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinWithNode(JoinWithNode message) {
        ActorRef<ADBLargeMessageActor.Command> session = this.spawnNodeJoin(message.context);
        session.tell(new ADBNodeJoin.Execute());
        return Behaviors.same();
    }

    private Behavior<Command> handleTakeOverWork(TakeOverWork command) {
        if (command.joinTasks.size() < 1) {
            this.getContext().getSelf().tell(new RequestNextPartitions());
            return Behaviors.same();
        }
        this.workToTakeOver.addAll(command.joinTasks);
        if (!command.isLast()) {
            return Behaviors.same();
        }
        this.getContext().getLog().info("Taking over {} tasks of executor {}",
                this.workToTakeOver.size(), command.context.getExecutorNodeId());
        ActorRef<ADBLargeMessageActor.Command> session = this.spawnNodeJoin(command.context);
        session.tell(new ADBNodeJoin.TakeOverWork(this.workToTakeOver));
        this.workToTakeOver = new ObjectArrayList<>();
        return Behaviors.same();
    }

    private Behavior<Command> handleHandOverWork(HandOverWork command) {
        if (this.activeJoinSessions.size() < 1) {
            command.respondTo.tell(new ADBSlaveJoinSession.TakeOverWork(null, Collections.emptyList(), true));
            return Behaviors.same();
        }
        this.activeJoinSessions.get(this.activeJoinSessions.size() - 1)
                               .tell(new ADBNodeJoin.HandOverWork(command.respondTo));
        return Behaviors.same();
    }

    private ActorRef<ADBNodeJoin.Command> spawnNodeJoin(ADBNodeJoinContext context) {
        String sessionName = ADBNodeJoinFactory.sessionName(context);
        val behavior = ADBNodeJoinFactory.createDefault((ADBJoinQueryContext) this.queryContext,
                this.getContext().getSelf(), context);
        ActorRef<ADBNodeJoin.Command> session = this.getContext().spawn(behavior, sessionName);
        this.getContext().watchWith(session, new InterNodeSessionTerminated(session));
        this.activeJoinSessions.add(session);
        return session;
    }

    private Behavior<Command> handleJoinWithNodeResults(JoinPartitionsResults command) {
        if (command.getJoinCandidates().size() < 1) {
            return Behaviors.same();
        }
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

    private Behavior<Command> handleStealWorkFrom(StealWorkFrom command) {
        command.target.tell(new HandOverWork(this.getContext().getSelf()));
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Join Query";
    }
}
