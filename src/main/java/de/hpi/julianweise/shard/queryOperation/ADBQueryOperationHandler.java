package de.hpi.julianweise.shard.queryOperation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.NoArgsConstructor;

import java.util.Map;

public abstract class ADBQueryOperationHandler extends AbstractBehavior<ADBQueryOperationHandler.Command> {

    public interface Command extends CborSerializable {}

    @NoArgsConstructor
    public static class Execute implements Command {}


    protected final ActorRef<ADBShardInquirer.Command> client;
    protected final ADBQuery query;
    protected final Map<ADBKey, ADBEntityType> data;

    protected final int transactionId;

    public ADBQueryOperationHandler(ActorContext<ADBQueryOperationHandler.Command> context,
                                    ActorRef<ADBShardInquirer.Command> client, int transactionId, ADBQuery query,
                                    final Map<ADBKey, ADBEntityType> data) {
        super(context);
        this.data = data;
        this.client = client;
        this.transactionId = transactionId;
        this.query = query;
    }

    protected Behavior<Command> concludeTransaction() {
        this.client.tell(new ADBShardInquirer.ConcludeTransaction(transactionId));
        return Behaviors.stopped();
    }
}
