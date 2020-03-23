package de.hpi.julianweise.shard.queryOperation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.NoArgsConstructor;

import java.util.List;

public abstract class ADBQuerySessionHandler extends AbstractBehavior<ADBQuerySessionHandler.Command> {

    public interface Command extends CborSerializable {
    }

    @NoArgsConstructor
    public static class Execute implements Command {
    }


    protected final ActorRef<ADBQuerySession.Command> client;
    protected final ADBQuery query;
    protected final List<ADBEntityType> data;
    protected final ActorRef<ADBShard.Command> shard;
    protected final int globalShardId;
    protected final int transactionId;

    public ADBQuerySessionHandler(ActorContext<ADBQuerySessionHandler.Command> context,
                                  ActorRef<ADBShard.Command> shard,
                                  ActorRef<ADBQuerySession.Command> client, int transactionId, ADBQuery query,
                                  final List<ADBEntityType> data,
                                  int globalShardId) {
        super(context);
        this.data = data;
        this.shard = shard;
        this.client = client;
        this.transactionId = transactionId;
        this.globalShardId = globalShardId;
        this.query = query;

        this.getContext().getLog().info(String.format("Started QuerySessionHandler for transaction %d to handle %s",
                this.transactionId, this.getQuerySessionName()));
    }

    protected void concludeTransaction() {
        this.client.tell(new ADBQuerySession.ConcludeTransaction(this.shard, transactionId));
        this.getContext().getLog().info(String.format("Concluding QuerySessionHandler for transaction %d handling %s",
                this.transactionId, this.getQuerySessionName()));
    }

    protected abstract String getQuerySessionName();
}
