package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.Exit3Bolt;


public class TopologyT3 {
    
    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("masterStream", spout);
        
        builder.setBolt("giveRank", 
                       new GiveRankBolt(), 
                       nbExecutors).shuffleGrouping("masterStream");
        
        builder.setBolt("exit", 
                       new Exit3Bolt(portOUTPUT), 
                       nbExecutors).shuffleGrouping("giveRank");
        
        Config config = new Config();
        
        StormSubmitter.submitTopology("topoT3", config, builder.createTopology());
    }
}