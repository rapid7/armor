package com.rapid7.armor.shard;

public class ShardIdDriver {

    public static void main(String[] args) {
        ModShardStrategy mss = new ModShardStrategy(16);
        System.out.println(mss.shardNum(args[0]));
    }
}
