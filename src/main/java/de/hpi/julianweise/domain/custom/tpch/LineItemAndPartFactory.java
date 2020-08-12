package de.hpi.julianweise.domain.custom.tpch;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.univocity.parsers.common.record.Record;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.ADBEntityFactory;

public class LineItemAndPartFactory implements ADBEntityFactory {

    @Override
    public Class<? extends ADBEntity> getTargetClass() {
        return LineItemAndPart.class;
    }

    @Override
    public ADBEntity build(Record record) {
        return LineItemAndPart.builder()
                .l_orderkey(record.getInt(0))
                .l_partkey(record.getInt(1))
                .l_suppkey(record.getInt(2))
                .l_linenumber(record.getInt(3))
                .l_quantity(record.getFloat(4))
                .l_extendedprice(record.getFloat(5))
                .l_discount(record.getFloat(6))
                .l_tax(record.getFloat(7))
                .l_returnflag(record.getChar(8))
                .l_linestatus(record.getChar(9))
                .l_shipdate(record.getString(10))
                .l_commitdate(record.getString(11))
                .l_receiptdate(record.getString(12))
                .l_shipinstruct(record.getString(13))
                .l_shipmode(record.getString(14))
                .l_comment(record.getString(15))
                .p_partkey(record.getInt(16))
                .p_name(record.getString(17))
                .p_mfgr(record.getString(18))
                .p_brand(record.getString(19))
                .p_type(record.getString(20))
                .p_size(record.getInt(21))
                .p_container(record.getString(22))
                .p_retailprice(record.getFloat(23))
                .p_comment(record.getString(24))
                .build();
    }

    @Override
    public JsonDeserializer<? extends ADBEntity> buildDeserializer() {
        return new LineItemAndPartDeserializer();
    }
}
