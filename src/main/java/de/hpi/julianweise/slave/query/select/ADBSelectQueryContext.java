package de.hpi.julianweise.slave.query.select;

import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.slave.query.ADBQueryContext;

public class ADBSelectQueryContext extends ADBQueryContext {
    public ADBSelectQueryContext(ADBQuery query, int transactionId) {
        super(query, transactionId);
    }
}
