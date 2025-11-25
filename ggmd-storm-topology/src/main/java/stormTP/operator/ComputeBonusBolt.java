package stormTP.operator;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.*;

public class ComputeBonusBolt extends BaseBasicBolt {
    
    private Map<Integer, TortoiseState> tortoiseStates = new HashMap<>();
    private static final int BONUS_INTERVAL = 15;
    private int totalParticipants = 0;
    
    private class TortoiseState {
        int topCount = 0;
        int totalScore = 0;
        String topsRange = "";
        int startTop = -1;
        int lastRank = -1;
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            int id = input.getIntegerByField("id");
            int top = input.getIntegerByField("top");
            String rang = input.getStringByField("rang");
            int total = input.getIntegerByField("total");
            
            if (totalParticipants == 0) {
                totalParticipants = total;
            }
            
            int rankValue = Integer.parseInt(rang.replace("ex", ""));
            
            TortoiseState state = tortoiseStates.computeIfAbsent(id, k -> new TortoiseState());
            
            if (state.startTop == -1) {
                state.startTop = top;
            }
            
            state.topCount++;
            state.lastRank = rankValue;
            
            if (state.topCount % BONUS_INTERVAL == 0) {
                int bonusPoints = totalParticipants - rankValue;
                state.totalScore += bonusPoints;
                
                state.topsRange = state.startTop + "-" + (state.startTop + BONUS_INTERVAL - 1);
                
                System.out.println("ComputeBonusBolt - Tortoise id=" + id + 
                    " tops=" + state.topsRange + 
                    " rang=" + rankValue + 
                    " bonus=" + bonusPoints + 
                    " totalScore=" + state.totalScore);
                
                collector.emit(new Values(
                    id,
                    state.topsRange,
                    state.totalScore
                ));
                
                state.startTop = top + 1;
            }
            
        } catch (Exception e) {
            System.err.println("Error in ComputeBonusBolt: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "tops", "score"));
    }
}