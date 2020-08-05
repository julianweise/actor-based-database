package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Builder
@Getter
public class ADBPartitionJoinTask {

    private final static Logger LOG = LoggerFactory.getLogger(ADBPartitionJoinTask.class);

    public ADBPartitionJoinTask(int leftPartitionId, int rightPartitionId,
                                ActorRef<ADBPartitionManager.Command> leftPartitionManager,
                                ActorRef<ADBPartitionManager.Command> rightPartitionManager,
                                ADBPartitionHeader leftHeader,
                                ADBPartitionHeader rightHeader) {
        this.leftPartitionId = leftPartitionId;
        this.rightPartitionId = rightPartitionId;
        this.leftPartitionManager = leftPartitionManager;
        this.rightPartitionManager = rightPartitionManager;
        this.leftHeader = leftHeader;
        this.rightHeader = rightHeader;
    }

    private final int leftPartitionId;
    private final int rightPartitionId;
    private final ActorRef<ADBPartitionManager.Command> leftPartitionManager;
    private final ActorRef<ADBPartitionManager.Command> rightPartitionManager;
    private final ADBPartitionHeader leftHeader;
    private final ADBPartitionHeader rightHeader;
}
