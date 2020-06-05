package de.hpi.julianweise.utility.internals;


@SuppressWarnings("SpellCheckingInspection")
public class ADBInternalIDHelper {

    // [NNNN | PPPPPPPPPPPP | EEEEEEEEEEEEEEEE]
    //   4   |      16      |        16

    private static final int SHIFT_NODE = 28;
    private static final int SHIFT_PARTITION = 16;

    private static final int BIT_MASK_NODE = 0xF0000000;
    private static final int BIT_MASK_PARTITION = 0x0FFFF000;
    private static final int BIT_MASK_ENTITY = 0x0000FFFF;

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