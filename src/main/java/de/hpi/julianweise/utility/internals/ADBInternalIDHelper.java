package de.hpi.julianweise.utility.internals;


public class ADBInternalIDHelper {

    // [NNNNNNNN | PPPPPPPP | EEEEEEEEEEEEEEEE]

    private static final int SHIFT_NODE = 24;
    private static final int SHIFT_PARTITION = 16;

    private static final int BIT_MASK_NODE = 0xFF000000;
    private static final int BIT_MASK_PARTITION = 0xFF0000;
    private static final int BIT_MASK_ENTITY = 0xFFFF;

    public static int createID(int nodeId, int partitionId, int entityId) {
        return (nodeId << SHIFT_NODE) | (partitionId << SHIFT_PARTITION) | entityId;
    }

    public static int getNodeId(int internalId) {
        return (internalId & BIT_MASK_NODE) >> SHIFT_NODE;
    }

    public static int getPartitionId(int internalId) {
        return (internalId & BIT_MASK_PARTITION) >> SHIFT_PARTITION;
    }

    public static int getEntityId(int internalId) {
        return internalId & BIT_MASK_ENTITY;
    }

}