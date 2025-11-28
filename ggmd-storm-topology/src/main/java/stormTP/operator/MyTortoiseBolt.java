package stormTP.operator;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class MyTortoiseBolt extends BaseBasicBolt {
    
    private int tortoiseId;
    private String tortoiseName;
    private long cellCounter;
    
    public MyTortoiseBolt(int tortoiseId, String tortoiseName) {
        this.tortoiseId = tortoiseId;
        this.tortoiseName = tortoiseName;
        this.cellCounter = 0;
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            String jsonStr = input.getString(0);
            
            int runnersStart = jsonStr.indexOf("[", jsonStr.indexOf("runners"));
            int runnersEnd = jsonStr.lastIndexOf("]");
            String runnersStr = jsonStr.substring(runnersStart + 1, runnersEnd);
            
            String[] runners = runnersStr.split("}\\s*,\\s*\\{");
            
            boolean foundMyTortoise = false;
            
            for (String runner : runners) {
                runner = runner.replaceAll("[{}\\[\\]]", "");
                
                int id = extractIntValue(runner, "id");
                
                if (id == tortoiseId) {
                    int top = extractIntValue(runner, "top");
                    int total = extractIntValue(runner, "total");
                    int maxcel = extractIntValue(runner, "maxcel");
                    
                    cellCounter++;
                    foundMyTortoise = true;
                    
                    System.out.println("Tortoise " + tortoiseName + " (id=" + id + ") - cells: " + cellCounter + ", top: " + top);
                }
            }
            
            if (foundMyTortoise) {
                String jsonWithId = tortoiseId + "|||" + jsonStr;
                collector.emit(new Values(jsonWithId));
            }
            
        } catch (Exception e) {
            System.err.println("Error in MyTortoiseBolt: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private int extractIntValue(String jsonStr, String key) {
        int keyIndex = jsonStr.indexOf("\"" + key + "\"");
        if (keyIndex == -1) {
            return 0;
        }
        int colonIndex = jsonStr.indexOf(":", keyIndex);
        int valueStart = colonIndex + 1;
        int valueEnd = jsonStr.indexOf(",", valueStart);
        if (valueEnd == -1) {
            valueEnd = jsonStr.length();
        }
        String value = jsonStr.substring(valueStart, valueEnd).trim();
        return Integer.parseInt(value);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}