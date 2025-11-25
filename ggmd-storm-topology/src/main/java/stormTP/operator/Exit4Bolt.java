package stormTP.operator;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import stormTP.stream.StreamEmiter;

public class Exit4Bolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private int port;
    private StreamEmiter semit;
    
    public Exit4Bolt(int port) {
        this.port = port;
        this.semit = new StreamEmiter(this.port);
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            // Récupérer les champs du tuple
            int id = input.getIntegerByField("id");
            String tops = input.getStringByField("tops");
            int score = input.getIntegerByField("score");
            
            // Créer le JSON
            String jsonString = String.format(
                "{\"id\":%d,\"tops\":\"%s\",\"score\":%d}",
                id, tops, score
            );
            
            // Envoyer via StreamEmiter (réseau)
            this.semit.send(jsonString);
            
            System.out.println("Exit4Bolt - Envoyé -> " + jsonString);
            
            // Acquitter le tuple
            collector.ack(input);
            
        } catch (Exception e) {
            System.err.println("Error in Exit4Bolt: " + e.getMessage());
            e.printStackTrace();
            collector.fail(input);
        }
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    
    @Override
    public void cleanup() {
        // Fermer proprement le StreamEmiter si nécessaire
    }
}