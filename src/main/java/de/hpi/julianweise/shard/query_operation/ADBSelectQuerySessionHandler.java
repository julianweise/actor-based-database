package de.hpi.julianweise.shard.query_operation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.select.ADBSelectQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;

import java.util.List;
import java.util.stream.Collectors;

public class ADBSelectQuerySessionHandler extends ADBQuerySessionHandler {

    public ADBSelectQuerySessionHandler(ActorContext<ADBQuerySessionHandler.Command> context,
                                        ActorRef<ADBShard.Command> shard,
                                        ActorRef<ADBQuerySession.Command> client,
                                        ActorRef<ADBLargeMessageReceiver.InitializeTransfer> clientLargeMessageReceiver,
                                        int transactionId,
                                        ADBSelectionQuery query,
                                        final List<ADBEntityType> data,
                                        int globalShardId) {
        super(context, shard, client, clientLargeMessageReceiver, transactionId, query, data, globalShardId);
    }

    @Override
    public Receive<ADBQuerySessionHandler.Command> createReceive() {
        return this.createReceiveBuilder()
                   .onMessage(Execute.class, this::handleExecute)
                   .build();
    }

    private Behavior<ADBQuerySessionHandler.Command> handleExecute(Execute command) {
        List<ADBEntityType> results = this.data
                .stream()
                .filter(entity -> entity.matches((ADBSelectionQuery) this.query))
                .collect(Collectors.toList());

        this.sendToSession(ADBSelectQuerySession.SelectQueryResults.builder()
                                                                   .results(results)
                                                                   .globalShardId(this.globalShardId)
                                                                   .transactionId(transactionId)
                                                                   .build(), results.size());
        this.concludeTransaction();
        return Behaviors.same();
    }

    @Override
    protected Behavior<ADBQuerySessionHandler.Command> handleLargeMessageSenderResponse(WrappedLargeMessageSenderResponse response) {
        if(this.openTransferSessions.decrementAndGet() < 1 && this.concluding) {
            this.sendTransactionConclusion();
            return Behaviors.stopped();
        }
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Select Query";
    }

}
