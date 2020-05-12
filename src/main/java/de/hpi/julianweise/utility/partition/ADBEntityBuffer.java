package de.hpi.julianweise.utility.partition;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.slave.partition.ADBPartition;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Getter
public class ADBEntityBuffer {

    private final PriorityQueue<ADBEntity> dataPartitionBuffer = new ObjectHeapPriorityQueue<>(Comparator.comparing(ADBEntity::getPrimaryKey));
    private final int maxPartitionSize;
    private int bufferSize = 0;

    public ADBEntityBuffer(int maxPartitionSize) {
        this.maxPartitionSize = maxPartitionSize;
    }

    public void add(ADBEntity entity) {
        this.dataPartitionBuffer.enqueue(entity);
        this.bufferSize += entity.getSize();
    }

    public boolean isNewPartitionReady() {
        return this.bufferSize > this.maxPartitionSize;
    }

    public List<ADBEntity> getPayloadForPartition() {
        int partitionSize = 0;
        ArrayList<ADBEntity> partitionPayload = new ArrayList<>();
        while(!this.dataPartitionBuffer.isEmpty() && partitionSize + this.dataPartitionBuffer.first().getSize() < this.maxPartitionSize) {
            partitionSize += this.dataPartitionBuffer.first().getSize();
            partitionPayload.add(this.dataPartitionBuffer.dequeue());
        }
        this.bufferSize -= partitionSize;
        partitionPayload.trimToSize();
        return partitionPayload;
    }
}
