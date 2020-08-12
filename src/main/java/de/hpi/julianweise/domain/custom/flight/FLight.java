package de.hpi.julianweise.domain.custom.flight;

import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FLight extends ADBEntity {

    public int FL_DATA;
    public String OP_UNIQUE_CARRIER;
    public short ORIGIN_AIRPORT_ID;
    public short DEST_AIRPORT_ID;
    public String CRS_DEP_TIME;
    public String DEP_TIME;
    public float DEP_DELAY;
    public String WHEELS_ON;
    public float TAXI_IN;
    public String CRS_ARR_TIME;
    public String ARR_TIME;
    public float ARR_DELAY;
    public float CRS_ELAPSED_TIME;
    public float ACTUAL_ELAPSED_TIME;
    public float DISTANCE;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBIntegerKey(this.getPrimaryKeyValue());
    }

    private int getPrimaryKeyValue() {
        final int prime = 31;
        int result = prime + Integer.hashCode(this.FL_DATA);
        result = prime * result + this.OP_UNIQUE_CARRIER.hashCode();
        result = prime * result + Short.hashCode(this.ORIGIN_AIRPORT_ID);
        result = prime * result + Short.hashCode(this.DEST_AIRPORT_ID);
        result = prime * result + this.CRS_DEP_TIME.hashCode();
        return result;
    }

    @Override
    public int getSize() {
        return Integer.BYTES + 2 * Short.BYTES + 6 * Float.BYTES +
                this.calculateStringMemoryFootprint(2) +
                this.calculateStringMemoryFootprint(4) +
                this.calculateStringMemoryFootprint(4) +
                this.calculateStringMemoryFootprint(4) +
                this.calculateStringMemoryFootprint(4) +
                this.calculateStringMemoryFootprint(4);
    }
}
