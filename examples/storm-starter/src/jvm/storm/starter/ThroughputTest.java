package storm.starter;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.indexingTopology.Config.Config;
import storm.starter.indexingTopology.exception.UnsupportedGenericException;
import javafx.util.Pair;
import storm.starter.indexingTopology.util.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
/**
 * Created by acelzj on 8/18/16.
 */
public class ThroughputTest {
    LinkedBlockingQueue<Pair> queue = new LinkedBlockingQueue<Pair>();
    BTree<Double, Integer> indexedData;
    File file;
    File outputFile;
    BufferedReader bufferedReader;
    MemChunk chunk;
    int bytesLimit;
    AtomicLong total;
    int btreeOrder;
    int numTuples;
    ByteArrayOutputStream bos;
    int numTuplesBeforeWritting;
    SplitCounterModule sm;
    TimingModule tm;
    double indexValue;
    FileOutputStream fop;
    byte[] bytes;
    //    CopyOnWriteArrayList<Long> timer = new CopyOnWriteArrayList<Long>();
    int chunkId;
    BulkLoader bulkLoader;
    BTree<Double, Integer> copyOfIndexedData;
    public ThroughputTest() {
        queue = new LinkedBlockingQueue<Pair>();
        file = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/input_data");
        outputFile = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/total_time_thread_base_line_4");
//        outputFile = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/total_time_thread_4");
        bytesLimit = 6500000;
        chunk = MemChunk.createNew(bytesLimit);
        tm = TimingModule.createNew();
        sm = SplitCounterModule.createNew();
        btreeOrder = 4;
        chunkId = 0;
        total = new AtomicLong(0);
        numTuples = 0;
        numTuplesBeforeWritting = 1;
        indexedData = new BTree<Double,Integer>(btreeOrder, tm, sm);
        bulkLoader = new BulkLoader(btreeOrder, tm, sm);
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fop = new FileOutputStream(outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Thread emitThread = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    String text = null;
                    try {
                        text = bufferedReader.readLine();
                        indexValue = Double.parseDouble(text);
                        ++numTuples;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    int offset = 0;
                    try {
                        offset = chunk.write(serializeIndexValue());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if (offset >= 0) {
                        Pair pair = new Pair(indexValue, offset);
                        try {
                            queue.put(pair);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        bulkLoader.addRecord(pair);
                    } else {
                        while (!queue.isEmpty()) {
                            Utils.sleep(1);
                        }
                        int processedTuples = numTuples - numTuplesBeforeWritting;
                        double percentage = (double) sm.getCounter() * 100 / (double) processedTuples;
//                        System.out.println("percentage is " + percentage);
                        copyTree();
//                        createNewTree(percentage, processedTuples);
                        indexedData.clearPayload();
                        numTuplesBeforeWritting = numTuples;
                        long totalTime = total.get();
                        bulkLoader.resetRecord();
//                        System.out.println("Average time is " + (double) totalTime / (double) processedTuples);
                        String content = "" + (double) totalTime / (double) processedTuples;
                        String newline = System.getProperty("line.separator");
                        byte[] contentInBytes = content.getBytes();
                        byte[] nextLineInBytes = newline.getBytes();
                        chunk = MemChunk.createNew(bytesLimit);
                        try {
                            fop.write(contentInBytes);
                            fop.write(nextLineInBytes);
                            offset = chunk.write(serializeIndexValue());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        Pair pair = new Pair(indexValue, offset);
                        bulkLoader.addRecord(pair);
                        sm.resetCounter();
                        try {
                            queue.put(pair);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        ++chunkId;
//                        tm.reset();
                        total = new AtomicLong(0);
                    }
                }
            }
        });
        emitThread.start();
        Thread indexThread = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    if (!queue.isEmpty()) {
                        try {
                            Pair pair = queue.take();
                            Double indexValue = (Double) pair.getKey();
                            Integer offset = (Integer) pair.getValue();
                            long start = System.nanoTime();
                            indexedData.insert(indexValue, offset);
                            total.addAndGet(System.nanoTime() - start);
                        } catch (UnsupportedGenericException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        indexThread.start();
    }

    public byte[] serializeIndexValue() throws IOException{
        bos = new ByteArrayOutputStream();
        byte[] b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE).putDouble(indexValue).array();
        bos.write(b);
        return bos.toByteArray();
    }

    private void copyTree() {
        if (chunkId == 0) {
            try {
                copyOfIndexedData = (BTree) indexedData.clone(indexedData);
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        } else {
            try {
                indexedData = (BTree) copyOfIndexedData.clone(copyOfIndexedData);
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }
    }

    private void createNewTree(double percentage, int processedTuples) {
        if (percentage > Config.rebuildTemplatePercentage) {
//            System.out.println("Hello");
            indexedData = bulkLoader.createTreeWithBulkLoading();
        }
    }


    public static void main(String[] args) {
        File file = new File("/home/lzj/IndexTopology_experiment/NormalDistribution/input_data");
        ThroughputTest test = new ThroughputTest();
    }
}
