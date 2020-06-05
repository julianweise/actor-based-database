package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ADBPartitionJoinTask {
    private final int leftPartitionId;
    private final int rightPartitionId;
    private ActorRef<ADBPartitionManager.Command> leftPartitionManager;
    private ActorRef<ADBPartitionManager.Command> rightPartitionManager;
}
