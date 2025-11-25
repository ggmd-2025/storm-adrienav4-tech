package stormTP.operator;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class Exit3Bolt extends BaseBasicBolt {
    
    private int outputPort;
    
    public Exit3Bolt(int outputPort) {
        this.outputPort = outputPort;
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            int id = input.getIntegerByField("id");
            int top = input.getIntegerByField("top");
            String rang = input.getStringByField("rang");
            int total = input.getIntegerByField("total");
            int maxcel = input.getIntegerByField("maxcel");
            
            String jsonString = String.format(
                "{\"id\":%d,\"top\":%d,\"rang\":\"%s\",\"total\":%d,\"maxcel\":%d}",
                id, top, rang, total, maxcel
            );
            
            collector.emit(new Values(jsonString));
            
            System.out.println("My tortue et son rang !! - Tortue id=" + id + " rang=" + rang + " : " + jsonString);
            
        } catch (Exception e) {
            System.err.println("Error in Exit3Bolt: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}