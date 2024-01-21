package pl.edu.mimuw.mapreduce.taskmanager;

public enum TMStatus {
    BEGINNING(0),
    SENT_MAPS(2),
    RESCHEDULED_MAPS_IF_STALE(3),
    FINISHED_CONCATENATION(4),
    SENT_REDUCES(5),
    RESCHEDULED_REDUCES_IF_STALE(6),
    FINISHED(7);

    private final Integer level;

    TMStatus(Integer level) {
        this.level = level;
    }

    public Integer getLevel() {
        return level;
    }

    public boolean furtherThan(TMStatus status) {
        return this.level < status.level;
    }
}
