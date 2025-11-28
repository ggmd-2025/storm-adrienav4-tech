package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.ComputeBonusBolt;
import stormTP.operator.Exit4Bolt;

public class TopologyT4 {
    
    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        int tortoiseId = Integer.parseInt(args[2]);
        
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("masterStream", spout);
        
        builder.setBolt("myTortoise", 
                       new MyTortoiseBolt(tortoiseId, "Paulo"), 
                       nbExecutors).shuffleGrouping("masterStream");
        builder.setBolt("giveRank", 
                       new GiveRankBolt(), 
                       nbExecutors).shuffleGrouping("myTortoise");
        builder.setBolt("computeBonus", 
                       new ComputeBonusBolt(), 
                       nbExecutors).shuffleGrouping("giveRank");
        builder.setBolt("exit", 
                       new Exit4Bolt(portOUTPUT), 
                       nbExecutors).shuffleGrouping("computeBonus");
        Config config = new Config();
        StormSubmitter.submitTopology("topoT4", config, builder.createTopology());
    }
}