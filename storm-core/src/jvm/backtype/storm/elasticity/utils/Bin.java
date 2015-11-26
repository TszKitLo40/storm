package backtype.storm.elasticity.utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A Bin holding integers.
 * <br/>
 * The number of items it can hold is not limited, but the added value of the
 * items it holds may not be higher than the given maximal size.
 */
public class Bin implements Comparator<Bin> {

    /**
     * maximal allowed added value of items.
     */
    protected int maxSize;
    /**
     * current added value of items.
     */
    protected int currentSize;
    /**
     * list of items in bin.
     */
    protected List<Integer> items;

    protected int index;

    /**
     * construct new bin with given maximal size.
     *
     * @param maxSize
     */
    public Bin(int maxSize) {
        this.maxSize = maxSize;
        this.currentSize = 0;
        this.items = new ArrayList<Integer>();
    }

    /**
     * adds given item to this bin, and increases the currentSize of the bin by
     * value of item. If item does not fit, it will not be put in the bin and
     * false will be returned.
     *
     * @param item item to put in bin
     * @return true if item fit in bin, false otherwise
     */
    public boolean put(Integer item) {
        if (currentSize + item <= maxSize) {
            items.add(item);
            currentSize += item;
            return true;
        } else {
            return false; // item didn't fit
        }
    }

    /**
     * removes given item from bin and reduces the currentSize of the bin by
     * value of item.
     *
     * @param item item to remove from bin
     */
    public void remove(Integer item) {
        items.remove(item);
        currentSize -= item;
    }

    /**
     * returns the number of items in this bin (NOT the added value of the
     * items).
     *
     * @return number of items in this bin
     */
    public int numberOfItems() {
        return items.size();
    }

    /**
     * creates a deep copy of this bin.
     *
     * @return deep copy of this bin
     */
    public Bin deepCopy() {
        Bin copy = new Bin(0);
        copy.items = new ArrayList<Integer>(items); // Integers are not copied by reference
        copy.currentSize = currentSize;
        copy.maxSize = maxSize;
        return copy;
    }

    @Override
    public String toString() {
        String res = "";
        for (int i = 0; i < items.size(); i++) {
            res += items.get(i) + " ";
        }
        res += "    Size: " + currentSize + " (max: " + maxSize + ")";
        return res;
    }

    @Override
    public int compare(Bin bin, Bin bin2) {
        return Integer.compare(bin.currentSize,bin2.currentSize);
    }


    public static class BinComparator implements Comparator<Bin> {

        @Override
        public int compare(Bin bin, Bin bin2) {
            return Integer.compare(bin.currentSize,bin2.currentSize);
        }
    }

    public static void main(String [] args) {
        Bin b1 = new Bin(100);
        Bin b2 = new Bin(100);
        Bin b3 = new Bin(100);

        b1.put(50);
        b2.put(30);
        b3.put(60);

        ArrayList<Bin> bins = new ArrayList<Bin>();
        bins.add(b1);
        bins.add(b2);
        bins.add(b3);

        Collections.sort(bins,new BinComparator());

        for(Bin b: bins) {
            System.out.println(b.currentSize);
        }
    }
}
