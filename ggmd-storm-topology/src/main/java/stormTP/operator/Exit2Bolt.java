package stormTP.operator;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Exit2Bolt extends BaseBasicBolt {
    
    private int outputPort;
    
    public Exit2Bolt(int outputPort) {
        this.outputPort = outputPort;
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            // Récupérer tous les champs du tuple
            int id = input.getIntegerByField("id");
            int top = input.getIntegerByField("top");
            String nom = input.getStringByField("nom");
            long nbCellsParcourus = input.getLongByField("nbCellsParcourus");
            int total = input.getIntegerByField("total");
            int maxcel = input.getIntegerByField("maxcel");
            
            String jsonString = String.format(
                "{\"id\":%d,\"top\":%d,\"nom\":\"%s\",\"nbCellsParcourus\":%d,\"total\":%d,\"maxcel\":%d}",
                id, top, nom, nbCellsParcourus, total, maxcel
            );
            
            collector.emit(new Values(jsonString));
            

            System.out.println("My Tortue !! - Tortue " + nom + " : " + jsonString);
            
        } catch (Exception e) {
            System.err.println("Error in Exit2Bolt: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}