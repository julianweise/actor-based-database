package de.hpi.julianweise.slave.query.join.node;

import akka.actor.typed.ActorRef;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.slave.partition.ADBPartition;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.ADBPartitionJoinExecutor;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@Builder
@Getter
public class ADBPartitionJoinTask {
    private final boolean reversed;
    private final ADBJoinQuery query;
    private final ActorRef<ADBPartition.Command> localPartition;
    private final Map<String, ObjectList<ADBEntityEntry>> foreignAttributes;
    private final ActorRef<ADBPartitionJoinExecutor.PartitionsJoined> respondTo;
}
