package top.byteinfo.iter.producer;

public class ChangedEvent {
    private String database;
    private String table;
    private Long ts;
    private Long xid;
    private String commit;
    private String data;
    public ChangedEvent(String database, String table, Long ts, Long xid, String commit, String data) {
        this.database = database;
        this.table = table;
        this.ts = ts;
        this.xid = xid;
        this.commit = commit;
        this.data = data;
    }
    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public Long getTs() {
        return ts;
    }

    public Long getXid() {
        return xid;
    }

    public String getCommit() {
        return commit;
    }

    public String getData() {
        return data;
    }


}
