package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Adapter;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSenderFactory;
import de.hpi.julianweise.utility.serialization.KryoSerializable;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBJoinWithNodeSessionHandler extends ADBLargeMessageActor {

    private final ActorRef<ADBJoinWithNodeSession.Command> session;
    private final ADBJoinQuery query;
    private final AtomicInteger localPartitionsToProvideAttributesFor = new AtomicInteger(0);
    private Map<Integer, Map<String, List<ADBComparable2IntPair>>> attributes;
    private Map<Integer, int[]> lPartitionIdsLeft;
    private Map<Integer, int[]> lPartitionIdsRight;
    private AtomicInteger numberOfExternalPartitionsToCheck;


    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    protected static class RequestJoinAttributes implements Command, KryoSerializable {
        List<ADBPartitionHeader> headers;
    }

    @AllArgsConstructor
    private static class RelevantPartitionsWrapper implements Command {
        private final ADBPartitionManager.RelevantPartitionsJoinQuery response;
    }

    @AllArgsConstructor
    private static class JoinAttributesWrapper implements Command {
        private final ADBPartition.JoinAttributes response;
    }


    @AllArgsConstructor
    public static class ConcludeSession implements Command, KryoSerializable {}

    public ADBJoinWithNodeSessionHandler(ActorContext<Command> context,
                                         ActorRef<ADBJoinWithNodeSession.Command> session, ADBQuery query,
                                         int remoteShardId) {
        super(context);
        session.tell(new ADBJoinWithNodeSession.RegisterHandler(this.getContext().getSelf()));

        this.session = session;
        this.attributes = new Object2ObjectLinkedOpenHashMap<>();
        this.query = (ADBJoinQuery) query;

        assert ADBPartitionManager.getInstance() != null : "Requesting ADBPartitionManager but not initialized yet";
        this.getContext().getLog().info("[CREATE] on shard #" + ADBSlave.ID + " for join with shard #" + remoteShardId);
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(RequestJoinAttributes.class, this::handleRequestJoinAttributes)
                   .onMessage(RelevantPartitionsWrapper.class, this::handleRelevantPartitions)
                   .onMessage(JoinAttributesWrapper.class, this::handleJoinAttributes)
                   .onMessage(ConcludeSession.class, this::handleConcludeSession)
                   .build();
    }

    @Override
    protected Behavior<Command> handleLargeMessageTransferCompleted(ADBLargeMessageSender.TransferCompleted response) {
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestJoinAttributes(RequestJoinAttributes command) {
        this.getContext().getLog().debug("Asked to provide join attributes for " + command.headers.size() + " headers");
        this.lPartitionIdsRight = new Int2ObjectLinkedOpenHashMap<>(command.headers.size());
        this.lPartitionIdsLeft = new Int2ObjectLinkedOpenHashMap<>(command.headers.size());
        this.attributes = new Object2ObjectLinkedOpenHashMap<>(command.headers.size());
        this.numberOfExternalPartitionsToCheck = new AtomicInteger(command.headers.size());

        val respondTo = getContext().messageAdapter(ADBPartitionManager.RelevantPartitionsJoinQuery.class,
                RelevantPartitionsWrapper::new);
        for (ADBPartitionHeader header : command.headers) {
            assert ADBPartitionManager.getInstance() != null : "Requesting ADBPartitionManager but not initialized yet";
            ADBPartitionManager.getInstance().tell(ADBPartitionManager.RequestPartitionsForJoinQuery
                    .builder()
                    .respondTo(respondTo)
                    .externalHeader(header)
                    .query(this.query)
                    .build());
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleRelevantPartitions(RelevantPartitionsWrapper wrapper) {
        this.numberOfExternalPartitionsToCheck.decrementAndGet();
        this.lPartitionIdsLeft.put(wrapper.response.getFPartitionId(), wrapper.response.getLPartitionIdsLeft());
        this.lPartitionIdsRight.put(wrapper.response.getFPartitionId(), wrapper.response.getLPartitionIdsRight());
        val respondTo = getContext().messageAdapter(ADBPartition.JoinAttributes.class, JoinAttributesWrapper::new);
        for(ActorRef<ADBPartition.Command> lPartition : wrapper.response.getPartitions()) {
            this.localPartitionsToProvideAttributesFor.incrementAndGet();
            lPartition.tell(new ADBPartition.RequestJoinAttributes(respondTo, this.query));
        }
        if (this.numberOfExternalPartitionsToCheck.get() < 1 && this.localPartitionsToProvideAttributesFor.get() < 1) {
            this.getContext().getLog().warn("No matching partitions have been found. Returning empty join candidates.");
            this.returnJoinCandidates();
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinAttributes(JoinAttributesWrapper wrapper) {
        this.attributes.put(wrapper.response.getPartitionId(), wrapper.response.getAttributes());
        this.localPartitionsToProvideAttributesFor.decrementAndGet();
        if (this.numberOfExternalPartitionsToCheck.get() < 1 && this.localPartitionsToProvideAttributesFor.get() < 1) {
            this.getContext().getLog().debug("Returning join candidates to " + this.session);
            this.returnJoinCandidates();
        }
        return Behaviors.same();
    }

    private void returnJoinCandidates() {
        val message  = new ADBJoinWithNodeSession.ForeignNodeAttributes(attributes, lPartitionIdsLeft, lPartitionIdsRight);
        val name = ADBLargeMessageSenderFactory.name(getContext().getSelf(), session, message.getClass(), "attributes");
        this.getContext().spawn(ADBLargeMessageSenderFactory.createDefault(message, largeMessageSenderWrapping), name)
            .tell(new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(this.session), message.getClass()));
    }

    private Behavior<Command> handleConcludeSession(ConcludeSession command) {
        if (this.localPartitionsToProvideAttributesFor.get() < 1) {
            this.getContext().getLog().info("Stopping handler ...");
            return Behaviors.stopped();
        }
        this.getContext().getLog().warn("Unable to stop handler. Still remaining attributes to send for local part.");
        this.getContext().scheduleOnce(Duration.ofMillis(50), this.getContext().getSelf(), command);
        return Behaviors.same();
    }
}
