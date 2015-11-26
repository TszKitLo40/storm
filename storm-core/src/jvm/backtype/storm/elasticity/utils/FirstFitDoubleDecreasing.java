package backtype.storm.elasticity.utils;

import java.util.*;

/**
 * Created by robert on 11/26/15.
 */
public class FirstFitDoubleDecreasing extends AbstractBinPacking {

    private List<Bin> bins = new ArrayList<Bin>();

    private int nbins;

    private Map<Integer, Integer> ballToBinMapping = new HashMap<Integer, Integer>();

    public FirstFitDoubleDecreasing(List<Integer> in, int nbins) {
        super(in, Integer.MAX_VALUE);
        this.nbins = nbins;
    }

    public Map<Integer, Integer> getBucketToPartitionMap() {
//        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

//        for(Bin b : bins) {
//            map.put()
//        }
        return ballToBinMapping;
    }


    @Override
    public int getResult() {
        bins.clear();
        ballToBinMapping.clear();
        for(int i=0; i<nbins; i++) {
            bins.add(new Bin(Integer.MAX_VALUE));
            bins.get(i).index = i;
        }

        Collections.sort(in, Collections.reverseOrder()); // sort input by size (big to small)

        Bin.BinComparator comparator = new Bin.BinComparator();
        for (int i = 0; i < in.size(); i++) {
            // iterate over bins and try to put the item into the first one it fits into
            Collections.sort(bins, comparator);
            boolean putItem = false; // did we put the item in a bin?

            for(Bin b: bins) {
                if(b.put(in.get(i))) {
                    putItem = true;
                    ballToBinMapping.put(i, b.index);
//                    System.out.println("put "+ i + "to"+b.index);
                    break;
                }
            }

            if(!putItem) {
                return -1;
            }
        }
        return bins.size();
    }

    @Override
    public void printBestBins() {

        System.out.println("Bins:");

        for (Bin bin : bins) {
            System.out.println(bin.toString());
        }

    }

    public String toString() {
        String ret = "";
        ret += "Bins: \n";

//        System.out.println("Bins:");

        for (Bin bin : bins) {
//            System.out.println(bin.toString());
            ret += bin.toString()+"\n";
        }

        ret +=getBucketToPartitionMap()+"\n";

        return ret;

    }
}
