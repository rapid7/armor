package com.rapid7.armor.xact;

public class ArmorXact {
    private String target;
    private String baseline;
    private Long time;
    private static final String SEPERATOR = ":";

    public String getBaseline() {
        return baseline;
    }
    public Long getTime() {
        return time;
    }
    public String getTarget() {
        return target;
    }
    public ArmorXact(String target, String baseline, Long time) {
        this.baseline = baseline;
        this.target = target;
        this.time = time;
    }
    public ArmorXact(String armorTransaction) {
        String[] split = armorTransaction.split(SEPERATOR);
        if (split.length != 3)
            throw new IllegalArgumentException("The armor transaction is invalid must come in 3 parts");
        target = split[0];
        baseline = split[1];
        time = Long.parseLong(split[2]);
    }
    
    @Override
    public String toString() {
        return target + SEPERATOR + baseline + SEPERATOR + time;
    }
}
