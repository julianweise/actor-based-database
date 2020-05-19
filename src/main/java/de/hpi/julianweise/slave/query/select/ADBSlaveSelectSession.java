package de.hpi.julianweise.slave.query.select;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query.select.ADBMasterSelectSession;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.slave.ADBSlave;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.slave.worker_pool.workload.SelectionQueryWorkload;
import de.hpi.julianweise.slave.worker_pool.workload.Workload;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.val;

import java.util.concurrent.atomic.AtomicInteger;


public class ADBSlaveSelectSession extends ADBSlaveQuerySession {

    private final ObjectList<ADBEntity> partialResults = new ObjectArrayList<>();
    private AtomicInteger partitionResultsPending;

    @AllArgsConstructor
    private static class RelevantPartitionsWrapper implements Command {
        private final ADBPartitionManager.RelevantPartitionsSelectionQuery response;
    }

    @AllArgsConstructor
    private static class PartitionDataWrapper implements Command {
        private final ADBPartition.Data response;
    }

    @AllArgsConstructor
    private static class SelectionWorkerWrapper implements Command {
        private final GenericWorker.Response response;
    }

    @AllArgsConstructor
    private static class ConcludeSession implements Command {}

    public ADBSlaveSelectSession(ActorContext<ADBSlaveQuerySession.Command> context,
                                 ActorRef<ADBMasterQuerySession.Command> client,
                                 ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientLargeMessageReceiver,
                                 int transactionId,
                                 ADBSelectionQuery query) {
        super(context, client, clientLargeMessageReceiver, transactionId, query);
    }

    @Override
    public Receive<ADBSlaveQuerySession.Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(Execute.class, this::handleExecute)
                   .onMessage(RelevantPartitionsWrapper.class, this::handleRelevantPartitions)
                   .onMessage(PartitionDataWrapper.class, this::handlePartitionData)
                   .onMessage(SelectionWorkerWrapper.class, this::handleWorkerResults)
                   .onMessage(ConcludeSession.class, this::handleConcludeSession)
                   .build();
    }

    private Behavior<ADBSlaveQuerySession.Command> handleExecute(Execute command) {
        val partitionsRef = this.getContext().messageAdapter(ADBPartitionManager.RelevantPartitionsSelectionQuery.class,
                RelevantPartitionsWrapper::new);
        assert ADBPartitionManager.getInstance() != null : "Requesting ADBPartitionManager but not initialized yet";
        ADBPartitionManager.getInstance().tell(new ADBPartitionManager.RequestPartitionsForSelectionQuery(
                partitionsRef, (ADBSelectionQuery) this.query));
        return Behaviors.same();
    }

    private Behavior<Command> handleRelevantPartitions(RelevantPartitionsWrapper wrapper) {
        val dataRef = this.getContext().messageAdapter(ADBPartition.Data.class, PartitionDataWrapper::new);
        this.partitionResultsPending = new AtomicInteger(wrapper.response.getPartitions().size());
        for (ActorRef<ADBPartition.Command> partition : wrapper.response.getPartitions()) {
            partition.tell(new ADBPartition.RequestData(dataRef));
        }
        if (wrapper.response.getPartitions().size() < 1) this.getContext().getSelf().tell(new ConcludeSession());
        return Behaviors.same();
    }

    private Behavior<Command> handlePartitionData(PartitionDataWrapper wrapper) {
        val resultRef = this.getContext().messageAdapter(GenericWorker.Response.class, SelectionWorkerWrapper::new);
        ADBSelectionQuery query = (ADBSelectionQuery) this.query;
        Workload workload = new SelectionQueryWorkload(wrapper.response.getData(), query);
        ADBQueryManager.getWorkerPool().tell(new GenericWorker.WorkloadMessage(resultRef, workload));
        return Behaviors.same();
    }

    private Behavior<Command> handleWorkerResults(SelectionWorkerWrapper wrapper) {
        if (!(wrapper.response instanceof SelectionQueryWorkload.Results)) {
            this.getContext().getLog().error("Received " + wrapper.response.getClass().getName() + " instead of " +
                    "expected SelectionQueryWorkload results");
            return Behaviors.same();
        }
        this.partialResults.addAll(((SelectionQueryWorkload.Results) wrapper.response).getResults());
        if (this.partitionResultsPending.decrementAndGet() < 1) this.getContext().getSelf().tell(new ConcludeSession());
        return Behaviors.same();
    }

    @Override
    protected Behavior<ADBSlaveQuerySession.Command> handleLargeMessageSenderResponse(MessageSenderResponse response) {
        super.handleLargeMessageSenderResponse(response);
        this.concludeTransaction();
        return Behaviors.same();
    }

    private Behavior<Command> handleConcludeSession(ConcludeSession command) {
        this.sendToSession(ADBMasterSelectSession.SelectQueryResults.builder()
                                                                    .results(this.partialResults)
                                                                    .nodeId(ADBSlave.ID)
                                                                    .transactionId(transactionId)
                                                                    .build());
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Select Query";
    }

}
