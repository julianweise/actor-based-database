package de.hpi.julianweise.domain.custom.tpch;

import de.hpi.julianweise.domain.key.ADBIntegerKey;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LineItemAndPart extends ADBEntity {

    public int l_orderkey;
    public int l_partkey;
    public int l_suppkey;
    public int l_linenumber;
    public float l_quantity;
    public float l_extendedprice;
    public float l_discount;
    public float l_tax;
    public char l_returnflag;
    public char l_linestatus;
    public String l_shipdate;
    public String l_commitdate;
    public String l_receiptdate;
    public String l_shipinstruct;
    public String l_shipmode;
    public String l_comment;
    public int p_partkey;
    public String p_name;
    public String p_mfgr;
    public String p_brand;
    public String p_type;
    public int p_size;
    public String p_container;
    public float p_retailprice;
    public String p_comment;

    @Override
    public ADBKey getPrimaryKey() {
        return new ADBIntegerKey(100 * this.l_orderkey + this.l_linenumber);
    }

    @Override
    public int getSize() {
        return 6 * Integer.BYTES +
                5 * Float.BYTES +
                2 * Character.BYTES +
                this.calculateStringMemoryFootprint(25) +
                this.calculateStringMemoryFootprint(10) +
                this.calculateStringMemoryFootprint(44) +
                this.calculateStringMemoryFootprint(55) +
                this.calculateStringMemoryFootprint(25) +
                this.calculateStringMemoryFootprint(10) +
                this.calculateStringMemoryFootprint(25) +
                this.calculateStringMemoryFootprint(10) +
                this.calculateStringMemoryFootprint(23) +
                3 * this.calculateStringMemoryFootprint(10);
    }
}
