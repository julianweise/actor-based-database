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
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageActor;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSender;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageSenderFactory;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import de.hpi.julianweise.utility.largemessage.ADBSemiMaterializedPair;
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
import java.util.stream.Collectors;

public class ADBJoinWithNodeSessionHandler extends ADBLargeMessageActor {

    private final ActorRef<ADBJoinWithNodeSession.Command> session;
    private final ADBJoinQuery query;
    private final AtomicInteger numberOfLocalPartitionsToMaterialize = new AtomicInteger(0);
    private final AtomicInteger mandatesToMaterialize = new AtomicInteger(0);
    private final ActorRef<ADBSlaveQuerySession.Command> localQuerySessionHandler;
    private List<ActorRef<ADBPartition.Command>> allPartitions;
    private Map<Integer, Map<String, List<ADBPair<Comparable<Object>, Integer>>>> attributes;
    private Map<Integer, List<Integer>> joinCandidates;
    private AtomicInteger numberOfExternalPartitionsToCheck;


    @AllArgsConstructor
    @Getter
    @NoArgsConstructor
    protected static class RequestJoinAttributes implements Command, KryoSerializable {
        List<ADBPartitionHeader> headers;
    }

    @AllArgsConstructor
    private static class RelevantPartitionsWrapper implements Command {
        private final ADBPartitionManager.RelevantPartitions response;
    }

    @AllArgsConstructor
    private static class JoinAttributesWrapper implements Command {
        private final ADBPartition.JoinAttributes response;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    public static class MaterializeAndReturn implements Command, KryoSerializable {
        private int localPartitionId;
        private List<ADBSemiMaterializedPair> tuples;
        private boolean isReversed;
    }

    @AllArgsConstructor
    private static class MaterializedResultsWrapper implements Command {
        private final ADBPartition.MaterializedTuples results;
    }

    @AllArgsConstructor
    private static class AllPartitionsHeaderWrapper implements Command {
        private final ADBPartitionManager.AllPartitionsAndHeaders response;
    }

    @AllArgsConstructor
    public static class ConcludeSession implements Command, KryoSerializable {}

    public ADBJoinWithNodeSessionHandler(ActorContext<Command> context,
                                         ActorRef<ADBJoinWithNodeSession.Command> session, ADBQuery query,
                                         ActorRef<ADBSlaveQuerySession.Command> localQuerySessionHandler,
                                         int remoteShardId) {
        super(context);
        ADBQueryPerformanceSampler.log(true, this.getClass().getSimpleName(), "Join with Shard");
        session.tell(new ADBJoinWithNodeSession.RegisterHandler(this.getContext().getSelf()));

        this.session = session;
        this.localQuerySessionHandler = localQuerySessionHandler;
        this.attributes = new Object2ObjectLinkedOpenHashMap<>();
        this.query = (ADBJoinQuery) query;

        val respond = getContext().messageAdapter(ADBPartitionManager.AllPartitionsAndHeaders.class,
                AllPartitionsHeaderWrapper::new);
        assert ADBPartitionManager.getInstance() != null : "Requesting ADBPartitionManager but not initialized yet";
        ADBPartitionManager.getInstance().tell(new ADBPartitionManager.RequestAllPartitionsAndHeaders(respond));
        this.getContext().getLog().info("[CREATE] on shard #" + ADBSlave.ID + " for join with shard #" + remoteShardId);
    }

    @Override
    public Receive<Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(AllPartitionsHeaderWrapper.class, this::handleAllPartitions)
                   .onMessage(RequestJoinAttributes.class, this::handleRequestJoinAttributes)
                   .onMessage(RelevantPartitionsWrapper.class, this::handleRelevantPartitions)
                   .onMessage(JoinAttributesWrapper.class, this::handleJoinAttributes)
                   .onMessage(MaterializeAndReturn.class, this::handleMaterializeAndReturn)
                   .onMessage(MaterializedResultsWrapper.class, this::handleMaterializedResult)
                   .onMessage(ConcludeSession.class, this::handleConcludeSession)
                   .build();
    }

    @Override
    protected Behavior<Command> handleLargeMessageTransferCompleted(ADBLargeMessageSender.TransferCompleted response) {
        ADBQueryPerformanceSampler.log(false, this.getClass().getSimpleName(), "Join with Shard");
        return Behaviors.same();
    }

    private Behavior<Command> handleAllPartitions(AllPartitionsHeaderWrapper wrapper) {
        this.allPartitions = wrapper.response.getPartitions();
        return Behaviors.same();
    }

    private Behavior<Command> handleRequestJoinAttributes(RequestJoinAttributes command) {
        this.getContext().getLog().debug("Received request to provide matching join attributes for " + command.headers.size() + " headers");
        this.joinCandidates = new Int2ObjectLinkedOpenHashMap<>(command.headers.size());
        this.attributes = new Object2ObjectLinkedOpenHashMap<>(command.headers.size());
        this.numberOfExternalPartitionsToCheck = new AtomicInteger(command.headers.size());

        val respondTo = getContext().messageAdapter(ADBPartitionManager.RelevantPartitions.class, RelevantPartitionsWrapper::new);
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
        val lCandidates = wrapper.response.getPartitions().stream().map(ADBPair::getKey).collect(Collectors.toList());
        this.joinCandidates.put(wrapper.response.getExternalIndex(), lCandidates);
        val respondTo = getContext().messageAdapter(ADBPartition.JoinAttributes.class, JoinAttributesWrapper::new);
        for(ADBPair<Integer, ActorRef<ADBPartition.Command>> lPartition : wrapper.response.getPartitions()) {
            this.numberOfLocalPartitionsToMaterialize.incrementAndGet();
            lPartition.getValue().tell(new ADBPartition.RequestJoinAttributes(respondTo, this.query, lPartition.getKey()));
        }
        if (this.numberOfExternalPartitionsToCheck.get() < 1 && this.numberOfLocalPartitionsToMaterialize.get() < 1) {
            this.getContext().getLog().warn("No matching partitions have been found. Returning empty join candidates.");
            this.returnJoinCandidates();
        }
        return Behaviors.same();
    }

    private Behavior<Command> handleJoinAttributes(JoinAttributesWrapper wrapper) {
        this.attributes.put(wrapper.response.getPartitionIndex(), wrapper.response.getAttributes());
        this.numberOfLocalPartitionsToMaterialize.decrementAndGet();
        if (this.numberOfExternalPartitionsToCheck.get() < 1 && this.numberOfLocalPartitionsToMaterialize.get() < 1) {
            this.getContext().getLog().debug("Returning join candidates to " + this.session);
            this.returnJoinCandidates();
        }
        return Behaviors.same();
    }

    private void returnJoinCandidates() {
        val message  = new ADBJoinWithNodeSession.ForeignAttributes(this.attributes, this.joinCandidates);
        val name = ADBLargeMessageSenderFactory.name(getContext().getSelf(), session, message.getClass(), "attributes");
        this.getContext().spawn(ADBLargeMessageSenderFactory.createDefault(message, largeMessageSenderWrapping), name)
            .tell(new ADBLargeMessageSender.StartTransfer(Adapter.toClassic(this.session), message.getClass()));
    }

    private Behavior<Command> handleMaterializeAndReturn(MaterializeAndReturn command) {
        this.getContext().getLog().debug("Received request to materialize tuples from " + this.session);
        this.mandatesToMaterialize.incrementAndGet();
        val respondTo = getContext().messageAdapter(ADBPartition.MaterializedTuples.class,
                MaterializedResultsWrapper::new);
        ADBPartition.MaterializeTuples message = new ADBPartition.MaterializeTuples(respondTo, command.tuples,
                command.isReversed);
        ActorRef<ADBPartition.Command> localPartition = this.allPartitions.get(command.localPartitionId);
        assert localPartition != null : "Referred to an unknown local partition";
        localPartition.tell(message);
        return Behaviors.same();
    }

    private Behavior<Command> handleMaterializedResult(MaterializedResultsWrapper wrapper) {
        this.localQuerySessionHandler.tell(new ADBSlaveJoinSession.HandleJoinShardResults(wrapper.results.getResults()));
        this.mandatesToMaterialize.decrementAndGet();
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeSession(ConcludeSession command) {
        this.getContext().getLog().info("Received command to conclude this session from " + this.session);
        if (this.mandatesToMaterialize.get() < 1 && this.numberOfLocalPartitionsToMaterialize.get() < 1) {
            this.getContext().getLog().info("Stopping handler ...");
            return Behaviors.stopped();
        } else {
            String cause = this.mandatesToMaterialize.get() > 0 ? "tuples to fully materialize" : "join candidates " +
                    "to semi-materialize";
            this.getContext().getLog().warn("Unable to stop handler because of " + cause);
        }
        this.getContext().scheduleOnce(Duration.ofMillis(50), this.getContext().getSelf(), command);
        return Behaviors.same();
    }
}
