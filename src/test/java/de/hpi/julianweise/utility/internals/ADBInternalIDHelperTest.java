package de.hpi.julianweise.utility.internals;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBInternalIDHelperTest {

    @Test
    public void expectCorrectNodeId() {
        int nodeId = 3;
        int partitionId = 23;
        int entityId = 834;
        int objectUnderTest = ADBInternalIDHelper.createID(nodeId, partitionId, entityId);

        assertThat(ADBInternalIDHelper.getNodeId(objectUnderTest)).isEqualTo(nodeId);
    }

    @Test
    public void expectCorrectPartitionId() {
        int nodeId = 3;
        int partitionId = 23;
        int entityId = 834;
        int objectUnderTest = ADBInternalIDHelper.createID(nodeId, partitionId, entityId);

        assertThat(ADBInternalIDHelper.getPartitionId(objectUnderTest)).isEqualTo(partitionId);
    }

    @Test
    public void expectCorrectEntityId() {
        int nodeId = 3;
        int partitionId = 23;
        int entityId = 834;
        int objectUnderTest = ADBInternalIDHelper.createID(nodeId, partitionId, entityId);

        assertThat(ADBInternalIDHelper.getEntityId(objectUnderTest)).isEqualTo(entityId);
    }

}