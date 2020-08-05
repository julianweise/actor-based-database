package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
public class ADBNodeJoinContext {
    private int leftNodeId;
    private int rightNodeId;
    private int transactionId;
    private int executorNodeId;
    private ActorRef<ADBPartitionManager.Command> left;
    private ActorRef<ADBPartitionManager.Command> right;

    @Override
    public String toString() {
        return String.format("[ADBNodeJoinContext] tx: %d executorNodeId: %d leftNodeId: %d rightNodeId: %d",
                this.transactionId,
                this.executorNodeId,
                this.leftNodeId,
                this.rightNodeId
        );
    }
}
