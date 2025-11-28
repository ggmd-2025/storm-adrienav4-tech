package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.Exit2Bolt;

public class TopologyT2 {
    
    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        int tortoiseId = Integer.parseInt(args[2]);  // ID de la tortue à filtrer
        
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("masterStream", spout);
        builder.setBolt("myTortoise", 
                       new MyTortoiseBolt(tortoiseId, "Jhonny"), 
                       nbExecutors).shuffleGrouping("masterStream");
        builder.setBolt("exit", 
                       new Exit2Bolt(portOUTPUT), 
                       nbExecutors).shuffleGrouping("myTortoise");
        
        Config config = new Config();
        
        StormSubmitter.submitTopology("topoT2", config, builder.createTopology());
    }
}