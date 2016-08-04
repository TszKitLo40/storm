package backtype.storm.elasticity.utils.serialize;

import backtype.storm.elasticity.message.taksmessage.RemoteTupleExecuteResult;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoValuesDeserializer;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import com.esotericsoftware.kryo.io.Input;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by robert on 16-7-29.
 */
public class RemoteTupleExecuteResultDeserializer {
    GeneralTopologyContext _context;
    KryoValuesDeserializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Input _kryoInput;
    KryoTupleDeserializer tupleDeserializer;

    public RemoteTupleExecuteResultDeserializer(final Map conf, final GeneralTopologyContext context, KryoTupleDeserializer tupleDeserializer) {
        _kryo = new KryoValuesDeserializer(conf);
        _context = context;
        _ids = new SerializationFactory.IdDictionary(context.getRawTopology());
        _kryoInput = new Input(1);
        this.tupleDeserializer = tupleDeserializer;
    }

    public void deserialize(RemoteTupleExecuteResult tuple) {
//        System.out.println("Bytes total size: " + tuple.bytes.length);
        _kryoInput.setBuffer(tuple.bytes);
        tuple._taskId = _kryoInput.readInt(true);
        tuple._streamId = _kryoInput.readString();
        int byteLength = _kryoInput.readInt(true);
        if(byteLength>0) {
            byte[] bytes = _kryoInput.readBytes(byteLength);
//            System.out.println(String.format("inner byte length: %d(expected %d).", bytes.length, byteLength));
            tuple._inputTuple = tupleDeserializer.deserialize(bytes);
        }
        tuple._outputTuple = _kryo.deserializeFrom(_kryoInput);
        tuple._commandType = _kryoInput.readInt(true);
        tuple._originalTaskID = _kryoInput.readInt(true);
    }

    public RemoteTupleExecuteResult deserializeToTuple(byte[] bytes) {
        _kryoInput.setBuffer(bytes);
        int taskId = _kryoInput.readInt(true);
        String streamId = _kryoInput.readString();
        int byteLength = _kryoInput.readInt(true);
        Tuple inputTuple = null;
        if(byteLength > 0) {
            inputTuple = tupleDeserializer.deserialize(_kryoInput.readBytes(byteLength));
        }
        List<Object> outputTuple = _kryo.deserializeFrom(_kryoInput);
        int command = _kryoInput.readInt(true);
        int orignalTaskID = _kryoInput.readInt(true);
        RemoteTupleExecuteResult ret = new RemoteTupleExecuteResult(orignalTaskID,taskId, streamId, inputTuple, outputTuple, command);
        return ret;
    }
}
