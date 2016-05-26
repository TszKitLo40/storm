package storm.starter.poc;

import backtype.storm.elasticity.BaseElasticBolt;
import backtype.storm.elasticity.ElasticOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by robert on 25/5/16.
 */
public class ComputationIntensiveTransactionBolt extends BaseElasticBolt{

    public static class State{

        public Map<Long, Record> buys;
        public Map<Long, Record> sells;

        public State() {
            sells = new HashMap<>();
            buys = new HashMap<>();
        }

        public List<Record> getSells() {
            List<Record> list = new ArrayList<>(sells.values());
            Collections.sort(list, Record.getComparator());
            return list;
        }

        public List<Record> getBuys() {
            List<Record> list = new ArrayList<>(buys.values());
            Collections.sort(list, Record.getReverseComparator());
            return list;
        }



        public void insertBuy(Record record) {
            buys.put(record.orderNo, record);
        }

        public void insertSell(Record record) {
            sells.put(record.orderNo, record);
        }

        public void updateSell(Record record) {
            sells.put(record.orderNo, record);
        }

        public void updateBuy(Record record) {
            buys.put(record.orderNo, record);
        }

        public void removeBuy(Record record) {
            buys.remove(record.orderNo);
        }
        public void removeSell(Record record) {
            sells.remove(record.orderNo);
        }
    }
    @Override
    public Object getKey(Tuple tuple) {
        return tuple.getIntegerByField(PocTopology.SEC_CODE);
    }

    @Override
    public void execute(Tuple input, ElasticOutputCollector collector) {

        State state = (State)getValueByKey(getKey(input));
        if(state == null) {
            state = new State();
            setValueByKey(getKey(input), state);
        }

        Record newRecord = new Record(
                input.getLongByField(PocTopology.ORDER_NO),
                input.getStringByField(PocTopology.ACCT_ID),
                input.getDoubleByField(PocTopology.PRICE),
                input.getIntegerByField(PocTopology.VOLUME),
                input.getIntegerByField(PocTopology.SEC_CODE),
                input.getStringByField(PocTopology.TIME));

        if(input.getSourceStreamId().equals(PocTopology.BUYER_STREAM)) {

            List<Record> sells = state.getSells();

            for(Record sell: sells) {
                if(newRecord.volume == 0) {
                    break;
                }
                double tradeVolume = Math.min(newRecord.volume, sell.volume);
                newRecord.volume -= tradeVolume;
                sell.volume -= tradeVolume;
                state.updateSell(sell);
                System.out.println(String.format("User %s buys %f volume %s stock from User %s", newRecord.accountId, tradeVolume, newRecord.secCode, sell.accountId));
                if(sell.volume == 0) {
                    state.removeSell(sell);
                    System.out.println(String.format("Seller %s's transaction for stock %d! is completed!", sell.accountId, sell.secCode));
                }
            }
            if(newRecord.volume > 0) {
                state.insertBuy(newRecord);
            }

        } else {
            List<Record> buys = state.getBuys();

            for(Record buy: buys) {
                if(newRecord.volume == 0) {
                    break;
                }
                double tradeVolume = Math.min(newRecord.volume, buy.volume);
                newRecord.volume -= tradeVolume;
                buy.volume -= tradeVolume;
                state.updateBuy(buy);
                System.out.println(String.format("User %s sells %f volume %s stock to User %s", newRecord.accountId, tradeVolume, newRecord.secCode, buy.accountId));
                if(buy.volume == 0) {
                    state.removeBuy(buy);
                    System.out.println(String.format("Buyer %s's transaction for stock %d! is completed!", buy.accountId, buy.secCode));
                }
            }
            if(newRecord.volume > 0) {
                state.insertSell(newRecord);
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        declareStatefulOperator();
    }
}
