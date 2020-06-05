package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.ADBPartitionManager.RelevantPartitionsJoinQuery;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBJoinWithNodeSessionHandler extends ADBLargeMessageActor {

    private final ActorRef<ADBJoinWithNodeSession.Command> session;
    private final ADBJoinQuery query;
    private final AtomicInteger localPartitionsToProvideAttributesFor = new AtomicInteger(0);

    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    protected static class ProvideRelevantJoinPartitions implements Command, KryoSerializable {
        ADBPartitionHeader header;
    }

    @AllArgsConstructor
    private static class RelevantPartitionsWrapper implements Command {
        private final RelevantPartitionsJoinQuery response;
    }

    @AllArgsConstructor
    public static class ConcludeSession implements Command, KryoSerializable {
    }

    public ADBJoinWithNodeSessionHandler(ActorContext<Command> context,
                                         ActorRef<ADBJoinWithNodeSession.Command> session, ADBQuery query,
                                         int remoteShardId) {
        super(context);
        assert ADBPartitionManager.getInstance() != null : "Requiring ADBPartitionManager but not initialized yet";
        ADBQueryPerformanceSampler.log(true, "ADBJoinWithNodeSessionHandler", "start", this.hashCode());
        session.tell(new ADBJoinWithNodeSession.RegisterHandler(getContext().getSelf(), ADBPartitionManager.getInstance()));

        this.session = session;
        this.query = (ADBJoinQuery) query;

        this.getContext().getLog().info("[CREATE] on shard #" + ADBSlave.ID + " for join with shard #" + remoteShardId);
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(ProvideRelevantJoinPartitions.class, this::handleRequestJoinAttributes)
                   .onMessage(RelevantPartitionsWrapper.class, this::handleRelevantPartitions)
                   .onMessage(ConcludeSession.class, this::handleConcludeSession)
                   .build();
    }

    private Behavior<Command> handleRequestJoinAttributes(ProvideRelevantJoinPartitions command) {
        val respondTo = getContext().messageAdapter(RelevantPartitionsJoinQuery.class, RelevantPartitionsWrapper::new);
        ADBPartitionManager.getInstance().tell(ADBPartitionManager.RequestPartitionsForJoinQuery
                .builder()
                .respondTo(respondTo)
                .externalHeader(command.header)
                .query(this.query)
                .build());
        return Behaviors.same();
    }

    private Behavior<Command> handleRelevantPartitions(RelevantPartitionsWrapper wrapper) {
        this.session.tell(ADBJoinWithNodeSession.RelevantJoinPartitions
                .builder()
                .lPartitionId(wrapper.response.getFPartitionId())
                .fPartitionIdLeft(wrapper.response.getLPartitionIdsLeft())
                .fPartitionIdRight(wrapper.response.getLPartitionIdsRight())
                .build());
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeSession(ConcludeSession command) {
        if (this.localPartitionsToProvideAttributesFor.get() < 1) {
            this.getContext().getLog().info("Stopping handler ...");
            ADBQueryPerformanceSampler.log(false, "ADBJoinWithNodeSessionHandler", "stop", this.hashCode());
            return Behaviors.stopped();
        }
        this.getContext().getLog().warn("Unable to stop handler. Still remaining attributes to send for local part.");
        this.getContext().scheduleOnce(Duration.ofMillis(50), this.getContext().getSelf(), command);
        return Behaviors.same();
    }
}
