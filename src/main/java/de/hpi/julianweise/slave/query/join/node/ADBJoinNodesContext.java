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
public class ADBJoinNodesContext {
    private int leftNodeId;
    private int rightNodeId;
    private ActorRef<ADBPartitionManager.Command> left;
    private ActorRef<ADBPartitionManager.Command> right;

    @Override
    public String toString() {
        return "NodeJoinContext: Node#" + this.leftNodeId + " (left) <-> Node#" + this.rightNodeId + " (right)";
    }
}
