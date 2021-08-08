package com.rapid7.armor.xact;

public class ArmorXact {
    private String target;
    private String baseline;
    private Long time;
    private boolean baselineAuto;
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
    public ArmorXact(String target, String baseline, Long time, boolean baselineAuto) {
        this.baseline = baseline;
        this.target = target;
        this.time = time;
    }

    public ArmorXact(String armorTransaction) {
        String[] split = armorTransaction.split(SEPERATOR);
        if (split.length != 4)
            throw new IllegalArgumentException("The armor transaction is invalid must come in 3 parts");
        target = split[0];
        baseline = split[1];
        time = Long.parseLong(split[2]);
        baselineAuto = Boolean.parseBoolean(split[3]);
    }
    
    @Override
    public String toString() {
        return target + SEPERATOR + baseline + SEPERATOR + time + SEPERATOR + baselineAuto;
    }
}
