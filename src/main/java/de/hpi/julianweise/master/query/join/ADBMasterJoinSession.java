package de.hpi.julianweise.master.query.join;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.master.ADBMaster;
import de.hpi.julianweise.master.materialization.ADBJoinResultMaterializer;
import de.hpi.julianweise.master.materialization.ADBJoinResultMaterializerFactory;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.ADBSlaveQuerySession;
import de.hpi.julianweise.slave.query.join.ADBPartialJoinResult;
import de.hpi.julianweise.slave.query.join.ADBSlaveJoinSession;
import de.hpi.julianweise.slave.query.join.node.ADBJoinNodesContext;
import de.hpi.julianweise.utility.query.join.JoinExecutionPlan;
import de.hpi.julianweise.utility.serialization.CborSerializable;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.val;

import java.time.Duration;

public class ADBMasterJoinSession extends ADBMasterQuerySession {

    private final ActorRef<JoinExecutionPlan.Command> joinExecutionPlan;
    private final ADBJoinQuery query;
    private final ActorRef<ADBJoinResultMaterializer.Command> materializer;
    private boolean materializationCompleted;

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class RequestNextNodeToJoin implements ADBMasterQuerySession.Command, CborSerializable {
        private ActorRef<ADBSlaveQuerySession.Command> respondTo;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @SuperBuilder
    @Getter
    public static class JoinQueryResults extends ADBMasterQuerySession.QueryResults {
        private ADBPartialJoinResult joinResults;
    }

    @AllArgsConstructor
    public static class MaterializedJoinResultsWrapper implements Command {
        ADBJoinResultMaterializer.Response response;
    }

    @AllArgsConstructor
    public static class JoinExecutionPlanWrapper implements Command {
        JoinExecutionPlan.NextJoinNodePair response;
    }

    public ADBMasterJoinSession(ActorContext<ADBMasterQuerySession.Command> context,
                                ObjectList<ActorRef<ADBQueryManager.Command>> queryManagers,
                                ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManagers,
                                int transactionId,
                                ActorRef<ADBPartitionInquirer.Command> parent,
                                ADBJoinQuery query) {
        super(context, queryManagers, partitionManagers, transactionId, parent);
        this.joinExecutionPlan = getContext().spawn(JoinExecutionPlan.createDefault(partitionManagers, transactionId),
                "JoinExecPlan");
        this.materializer = this.initializeMaterializer();
        this.query = query;

        this.distributeQuery();
    }

    private ActorRef<ADBJoinResultMaterializer.Command> initializeMaterializer() {
        val respondTo = getContext().messageAdapter(ADBJoinResultMaterializer.Response.class, MaterializedJoinResultsWrapper::new);
        return getContext().spawn(ADBJoinResultMaterializerFactory.createDefault(partitionManagers, respondTo), "Materializer");
    }

    private void distributeQuery() {
        this.queryManagers.forEach(manager -> manager.tell(ADBQueryManager.QueryEntities
                .builder()
                .transactionId(transactionId)
                .query(query)
                .respondTo(this.getContext().getSelf())
                .build()));
    }

    @Override
    public Receive<ADBMasterQuerySession.Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(RequestNextNodeToJoin.class, this::handleRequestNextNodeComparison)
                   .onMessage(JoinQueryResults.class, this::handleJoinQueryResults)
                   .onMessage(MaterializedJoinResultsWrapper.class, this::handleMaterializedResults)
                   .onMessage(JoinExecutionPlanWrapper.class, this::handleJoinExecutionPlanResponse)
                   .build();
    }

    private Behavior<ADBMasterQuerySession.Command> handleRequestNextNodeComparison(RequestNextNodeToJoin command) {
        val respondTo = getContext().messageAdapter(JoinExecutionPlan.NextJoinNodePair.class, JoinExecutionPlanWrapper::new);
        this.joinExecutionPlan.tell(new JoinExecutionPlan.GetNextJoinNodePair(this.handlersToManager.get(command.respondTo), respondTo));
        return Behaviors.same();
    }

    private Behavior<ADBMasterQuerySession.Command> handleJoinExecutionPlanResponse(JoinExecutionPlanWrapper wrapper) {
        JoinExecutionPlan.NextJoinNodePair response = wrapper.response;
        if (!response.isHasNode()) {
            this.managerToHandlers.get(response.getRequestingPartitionManager()).tell(new ADBSlaveJoinSession.NoMoreNodesToJoinWith(this.transactionId));
            return Behaviors.same();
        }
        if (!this.managerToHandlers.containsKey(response.getRequestingPartitionManager())) {
            this.getContext().getLog().warn("No Node-Session mapping for {}",  response.getRightQueryManager());
            this.getContext().scheduleOnce(Duration.ofSeconds(1), this.getContext().getSelf(), wrapper);
            return Behaviors.same();
        }
        this.scheduleNextInterNodeJoin(this.managerToHandlers.get(response.getRequestingPartitionManager()),
                response.getLeftQueryManager(),
                response.getRightQueryManager());
        return Behaviors.same();
    }

    private void scheduleNextInterNodeJoin(ActorRef<ADBSlaveQuerySession.Command> requestingSession,
                                           ActorRef<ADBPartitionManager.Command> leftPartitionManager,
                                           ActorRef<ADBPartitionManager.Command> rightPartitionManager) {
        int leftNodeId = ADBMaster.getGlobalIdFor(leftPartitionManager);
        int rightNodeId = ADBMaster.getGlobalIdFor(rightPartitionManager);
        this.getContext().getLog().info("Asking Node#{} to join with Node#{}", leftNodeId, rightNodeId);
        requestingSession.tell(new ADBSlaveJoinSession.JoinWithNode(ADBJoinNodesContext
                .builder()
                .leftNodeId(leftNodeId)
                .rightNodeId(rightNodeId)
                .left(leftPartitionManager)
                .right(rightPartitionManager)
                .build()));
    }

    private Behavior<ADBMasterQuerySession.Command> handleJoinQueryResults(JoinQueryResults results) {
        if (!this.query.isShouldBeMaterialized()) {
            parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, results.joinResults,
                    results.joinResults.size(), false));
            return Behaviors.same();
        }
        this.materializer.tell(new ADBJoinResultMaterializer.MaterializeJoinResultChunk(results.joinResults));
        return Behaviors.same();
    }

    private Behavior<Command> handleMaterializedResults(MaterializedJoinResultsWrapper wrapper) {
        if (wrapper.response instanceof ADBJoinResultMaterializer.MaterializedResults) {
            val results = ((ADBJoinResultMaterializer.MaterializedResults) wrapper.response).getTuples();
            parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, results, results.size(), false));
        } else if (wrapper.response instanceof ADBJoinResultMaterializer.Concluded) {
            val results = new ObjectArrayList<>();
            parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, results, 0, true));
            this.materializationCompleted = true;
            this.conditionallyConcludeTransaction();
        }
        return Behaviors.same();
    }

    @Override
    protected Behavior<ADBMasterQuerySession.Command> handleConcludeTransaction(ConcludeTransaction command) {
        this.getContext().getLog().info(command.getSlaveQuerySession() + " concludes session");
        this.completedSessions.add(command.getSlaveQuerySession());
        if (this.completedSessions.size() == this.queryManagers.size()) {
            this.materializer.tell(new ADBJoinResultMaterializer.Conclude());
        }
        return this.conditionallyConcludeTransaction();
    }

    @Override
    protected String getQuerySessionName() {
        return "Join Query";
    }

    @Override
    protected void submitResults() {
        parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, new ObjectArrayList<>(), 0, true));
    }

    @Override
    protected boolean isFinalized() {
        if (!this.query.isShouldBeMaterialized()) {
            return true;
        }
        return this.materializationCompleted;
    }
}
