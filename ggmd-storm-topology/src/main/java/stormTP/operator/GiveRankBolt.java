package stormTP.operator;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.*;

public class GiveRankBolt extends BaseBasicBolt {
    
    private Map<Integer, Map<Integer, Integer>> topPositions = new HashMap<>();
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            // Lire le JSON avec le préfixe d'ID
            String prefixedJson = input.getStringByField("json");
            
            // Extraire l'ID et le JSON
            String[] parts = prefixedJson.split("\\|\\|\\|", 2);
            int myTortoiseId = Integer.parseInt(parts[0]);
            String jsonStr = parts[1];
            
            int runnersStart = jsonStr.indexOf("[", jsonStr.indexOf("runners"));
            int runnersEnd = jsonStr.lastIndexOf("]");
            String runnersStr = jsonStr.substring(runnersStart + 1, runnersEnd);
            
            String[] runners = runnersStr.split("}\\s*,\\s*\\{");
            
            int currentTop = -1;
            Map<Integer, Integer> positions = new HashMap<>();
            
            for (String runner : runners) {
                // Clean up the runner string
                runner = runner.replaceAll("[{}\\[\\]]", "");
                
                // Extract values
                int id = extractIntValue(runner, "id");
                int top = extractIntValue(runner, "top");
                int cellule = extractIntValue(runner, "cellule");
                
                if (currentTop == -1) {
                    currentTop = top;
                }
                
                positions.put(id, cellule);
            }

            if (currentTop != -1) {
                topPositions.put(currentTop, positions);
            }
            
            Map<Integer, Integer> rankMap = calculateRanks(positions);
            
            // N'émettre QUE pour ma tortue
            for (String runner : runners) {
                runner = runner.replaceAll("[{}\\[\\]]", "");
                
                int id = extractIntValue(runner, "id");
                
                // Filtrer uniquement ma tortue
                if (id == myTortoiseId) {
                    int top = extractIntValue(runner, "top");
                    int total = extractIntValue(runner, "total");
                    int maxcel = extractIntValue(runner, "maxcel");
                    
                    String rang = getRankString(rankMap, id, positions);
                    
                    collector.emit(new Values(
                        id,
                        top,
                        rang,
                        total,
                        maxcel
                    ));
                    
                    System.out.println("GiveRankBolt - Tortoise id=" + id + " - top=" + top + " - rang=" + rang);
                    break; // On a trouvé notre tortue, pas besoin de continuer
                }
            }
            
        } catch (Exception e) {
            System.err.println("Error in GiveRankBolt: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private Map<Integer, Integer> calculateRanks(Map<Integer, Integer> positions) {
        Map<Integer, Integer> rankMap = new HashMap<>();
        
        List<Map.Entry<Integer, Integer>> sortedList = new ArrayList<>(positions.entrySet());
        sortedList.sort((a, b) -> Integer.compare(b.getValue(), a.getValue()));
        
        for (int i = 0; i < sortedList.size(); i++) {
            rankMap.put(sortedList.get(i).getKey(), i + 1);
        }
        
        return rankMap;
    }
    
    private String getRankString(Map<Integer, Integer> rankMap, int id, Map<Integer, Integer> positions) {
        int rank = rankMap.get(id);
        int position = positions.get(id);
        
        // Check for ties (other tortoise with same position)
        boolean hasTie = positions.values().stream()
            .filter(p -> !p.equals(position))
            .count() < positions.size() - 1;
        
        if (hasTie) {
            long tiesCount = positions.values().stream()
                .filter(p -> p.equals(position))
                .count();
            if (tiesCount > 1) {
                return rank + "ex";
            }
        }
        
        return String.valueOf(rank);
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
        declarer.declare(new Fields("id", "top", "rang", "total", "maxcel"));
    }
}