package de.hpi.julianweise.slave.query.join;

import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.slave.query.ADBQueryContext;
import lombok.Getter;

@Getter
public class ADBJoinQueryContext extends ADBQueryContext {

    public ADBJoinQueryContext(ADBQuery query, int transactionId) {
        super(query, transactionId);
    }

    public ADBJoinQuery getQuery() {
        return (ADBJoinQuery) this.query;
    }
}
