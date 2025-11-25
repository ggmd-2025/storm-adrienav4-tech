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
        
        /* Création du spout */
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);
        
        /* Création de la topologie */
        TopologyBuilder builder = new TopologyBuilder();
        
        /* Affectation à la topologie du spout */
        builder.setSpout("masterStream", spout);
        
        /* Affectation du bolt de filtrage de tortue */
        builder.setBolt("myTortoise", 
                       new MyTortoiseBolt(tortoiseId, "Donatello"), 
                       nbExecutors).shuffleGrouping("masterStream");
        
        /* Affectation du bolt de conversion JSON */
        builder.setBolt("exit", 
                       new Exit2Bolt(portOUTPUT), 
                       nbExecutors).shuffleGrouping("myTortoise");
        
        /* Création d'une configuration */
        Config config = new Config();
        
        /* La topologie est soumise à STORM */
        StormSubmitter.submitTopology("topoT2", config, builder.createTopology());
    }
}