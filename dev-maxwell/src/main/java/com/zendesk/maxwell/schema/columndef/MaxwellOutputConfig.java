package com.zendesk.maxwell.schema.columndef;

public class MaxwellOutputConfig {
    public boolean zeroDatesAsNull;

    public MaxwellOutputConfig(boolean zeroDatesAsNull) {
        this.zeroDatesAsNull = zeroDatesAsNull;
    }

    public MaxwellOutputConfig() {
        this.zeroDatesAsNull = true;
    }
}
