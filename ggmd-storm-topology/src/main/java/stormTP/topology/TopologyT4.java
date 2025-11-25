package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.ComputeBonusBolt;
import stormTP.operator.Exit4Bolt;

/**
 * Topologie pour tester le calcul des points bonus
 * Flux: InputStreamSpout -> MyTortoiseBolt -> GiveRankBolt -> ComputeBonusBolt -> Exit4Bolt
 * @author lumineau
 */
public class TopologyT4 {
    
    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        int tortoiseId = Integer.parseInt(args[2]);
        
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
        
        builder.setBolt("giveRank", 
                       new GiveRankBolt(), 
                       nbExecutors).shuffleGrouping("myTortoise");
        
        /* Affectation du bolt de calcul de bonus */
        builder.setBolt("computeBonus", 
                       new ComputeBonusBolt(), 
                       nbExecutors).shuffleGrouping("giveRank");
        
        /* Affectation du bolt de conversion JSON */
        builder.setBolt("exit", 
                       new Exit4Bolt(portOUTPUT), 
                       nbExecutors).shuffleGrouping("computeBonus");
        
        /* Création d'une configuration */
        Config config = new Config();
        
        /* La topologie est soumise à STORM */
        StormSubmitter.submitTopology("topoT4", config, builder.createTopology());
    }
}