package de.hpi.julianweise.utility.partition;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.Getter;

import java.util.Comparator;

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

    public ObjectList<ADBEntity> getPayloadForPartition() {
        int partitionSize = 0;
        ObjectArrayList<ADBEntity> partitionPayload = new ObjectArrayList<>();
        while(!this.dataPartitionBuffer.isEmpty() && partitionSize + this.dataPartitionBuffer.first().getSize() < this.maxPartitionSize) {
            partitionSize += this.dataPartitionBuffer.first().getSize();
            partitionPayload.add(this.dataPartitionBuffer.dequeue());
        }
        this.bufferSize -= partitionSize;
        partitionPayload.trim();
        return partitionPayload;
    }
}
