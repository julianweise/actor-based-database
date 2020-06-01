package de.hpi.julianweise.slave.query;

import de.hpi.julianweise.query.ADBQuery;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@AllArgsConstructor
@SuperBuilder
@Getter
public class ADBQueryContext {
    protected final ADBQuery query;
    protected final int transactionId;
}
