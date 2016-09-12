package storm.starter.indexingTopology.util;

import javafx.util.Pair;
import storm.starter.indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> implements Serializable {
	protected ArrayList<ArrayList<TValue>> values;
	protected double templateOverflowPercentage;

	
	public BTreeLeafNode(int order, BytesCounter counter) {
        super(order,counter);
        this.keys = new ArrayList<TKey>();
		this.values = new ArrayList<ArrayList<TValue>>();
	}

	public BTreeLeafNode(BTreeNode oldNode) throws CloneNotSupportedException{
		super(oldNode.ORDER, (BytesCounter) oldNode.counter.clone());
        this.keys = new ArrayList<TKey>();
		this.keys.addAll(oldNode.keys);

	/*	for (TKey key : keys) {
			System.out.print(key);
		}*/
	//	System.out.println();
	}

	@SuppressWarnings("unchecked")
	public ArrayList<TValue> getValueList(int index) {
		return this.values.get(index);
	}

	public void setValueList(int index, ArrayList<TValue> value) {
        if (index < this.values.size())
		    this.values.set(index,value);
        else if (index == this.values.size()) {
			// TODO fix this
//            this.counter.countValueAddition(value.length);
            this.values.add(index, value);
        }
        else
            throw new ArrayIndexOutOfBoundsException("index out of bounds");
	}
	
	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}
	
	@Override
    // TODO optimize to binary search
//	public int search(TKey key) {
//		for (int i = 0; i < this.getKeyCount(); ++i) {
//			 int cmp = this.getKey(i).compareTo(key);
//			 if (cmp == 0) {
//				 return i;
//			 }
//			 else if (cmp > 0) {
//				 return -1;
//			 }
//		}
//		return -1;
//	}

	public int search(TKey key) {
		int low = 0;
		int high = this.getKeyCount() - 1;
	    while (low <= high) {
			int mid = (low + high) >> 1;
			int cmp = this.getKey(mid).compareTo(key);
			if (cmp == 0) {
				return mid;
			} else if (cmp > 0) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}

		return -1;
	}

	

    public Collection<BTreeNode<TKey>> recursiveSerialize(ByteBuffer allocatedBuffer) {
		// TODO fix this
//        allocatedBuffer.put((byte) 'l');
//        allocatedBuffer.putInt(this.getKeyCount());
//        for (int i=0;i<this.keys.size();i++) {
//            try {
//                UtilGenerics.putIntoByteBuffer(allocatedBuffer,this.keys.get(i));
//            } catch (UnsupportedGenericException e) {
//                e.printStackTrace();
//            }
//
//            allocatedBuffer.putInt(this.values.get(i).length);
//            allocatedBuffer.put(this.values.get(i));
//        }
//
        return null;
    }

	/* The codes below are used to support insertion operation */
	
/*	public void insertKeyValue(TKey key, TValue value, TimingModule tm) throws UnsupportedGenericException {
//		int index = 0;
//		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
//			++index;
//		tm.startTiming(Constants.TIME_SEARCH_INDEX.str);
//		int index = searchIndex(key);
//		tm.endTiming(Constants.TIME_SEARCH_INDEX.str);
//
//		tm.startTiming(Constants.TIME_INSERT_INTO_ARRAYLIST.str);
//		if (index<this.keys.size() && this.getKey(index).compareTo(key)==0) {
//			this.values.get(index).add(value);
//		} else {
//			this.keys.add(index, key);
//			this.values.add(index, new ArrayList<TValue>(Arrays.asList(value)));
//			++this.keyCount;
//		}
		keys.add(key);
		values.add(new ArrayList<TValue>(Arrays.asList(value)));
		++keyCount;
//		tm.endTiming(Constants.TIME_INSERT_INTO_ARRAYLIST.str);
	}*/

	public void insertKeyValue(TKey key, TValue value, TimingModule tm) throws UnsupportedGenericException {
//		int index = 0;
//		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
//			++index;
//		tm.startTiming(Constants.TIME_SEARCH_INDEX.str);
//		int index = searchIndex(key);
//		tm.endTiming(Constants.TIME_SEARCH_INDEX.str);
//
//		tm.startTiming(Constants.TIME_INSERT_INTO_ARRAYLIST.str);
//		if (index<this.keys.size() && this.getKey(index).compareTo(key)==0) {
//			this.values.get(index).add(value);
//		} else {
//			this.keys.add(index, key);
//			this.values.add(index, new ArrayList<TValue>(Arrays.asList(value)));
//			++this.keyCount;
//		}
		keys.add(key);
		values.add(new ArrayList<TValue>(Arrays.asList(value)));
		++keyCount;
	}

	private int searchIndex(TKey key) {
		int low = 0;
		int high = this.getKeyCount() - 1;
		while (low <= high) {
			int mid = (low + high) >> 1;
			int cmp = this.getKey(mid).compareTo(key);
			if (cmp == 0) {
				return mid;
			} else if (cmp > 0) {
				high = mid - 1;
			} else {
				low = mid + 1;
			}
		}
		return low;
	}

	public void insertKeyValueList(TKey key, ArrayList<TValue> values) throws UnsupportedGenericException {
//		int index = 0;
//		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
//	  		++index;
        int index = searchIndex(key);
		if (index<this.keys.size() && this.getKey(index).compareTo(key)==0) {
			this.values.get(index).addAll(values);
		} else {
			this.keys.add(index, key);
			this.values.add(index, new ArrayList<TValue>(values));
			++this.keyCount;
		}
	}

	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {

		Collections.sort(keys);

		int midIndex = this.getKeyCount() / 2;
		
		BTreeLeafNode<TKey,TValue> newRNode = new BTreeLeafNode<TKey,TValue>(this.ORDER,counter);
		for (int i = midIndex; i < this.getKeyCount(); ++i) {
            try {
                newRNode.setKey(i - midIndex, this.getKey(i));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
            newRNode.setValueList(i - midIndex, this.getValueList(i));
		}

		newRNode.keyCount = this.getKeyCount() - midIndex;

        for (int i = this.getKeyCount()-1; i >= midIndex; i--)
            this.deleteAt(i);

        this.keyCount = midIndex;
		return newRNode;
	}
	
	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode, SplitCounterModule sm, BTreeLeafNode leaf) {
		throw new UnsupportedOperationException();
	}
	
	
	
	
	/* The codes below are used to support deletion operation */
	public boolean delete(TKey key) {
		int index = this.search(key);
		if (index == -1)
			return false;
//		System.out.println(index);
		this.deleteAt(index);
		return true;
	}

	public void deleteKeyValue(TKey key, TValue value) throws UnsupportedGenericException {
		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
			++index;

		if (index<this.keys.size() && this.getKey(index).compareTo(key)==0) {
			int indexLast = this.values.get(index).lastIndexOf(value);
			this.values.get(index).remove(indexLast);
		}
	}
	
	private void deleteAt(int index) {
        try {
            counter.countKeyRemoval(UtilGenerics.sizeOf(this.keys.get(index).getClass()));
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }

		// TODO fix this
//        counter.countValueRemoval(this.values.get(index).length);
        this.keys.remove(index);
        this.values.remove(index);
		--this.keyCount;
	}
	
	@Override
	protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Notice that the key sunk from parent is be abandoned. 
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
		BTreeLeafNode<TKey,TValue> siblingLeaf = (BTreeLeafNode<TKey,TValue>)rightSibling;
		
		int j = this.getKeyCount();
		for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
            try {
                this.setKey(j + i, siblingLeaf.getKey(i));
            } catch (UnsupportedGenericException e) {
                e.printStackTrace();
            }
            this.setValueList(j + i, siblingLeaf.getValueList(i));
		}
		this.keyCount += siblingLeaf.getKeyCount();
		
		this.setRightSibling(siblingLeaf.rightSibling);
		if (siblingLeaf.rightSibling != null)
			siblingLeaf.rightSibling.setLeftSibling(this);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
		BTreeLeafNode<TKey,TValue> siblingNode = (BTreeLeafNode<TKey,TValue>)sibling;
        try {
            this.insertKeyValueList(siblingNode.getKey(borrowIndex), siblingNode.getValueList(borrowIndex));
        } catch (UnsupportedGenericException e) {
            e.printStackTrace();
        }
        siblingNode.deleteAt(borrowIndex);
		
		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}

    public List<TValue> searchRange(TKey leftKey, TKey rightKey) {
        // find first index satisfying range
        int firstIndex;
        for (firstIndex=0;firstIndex<this.getKeyCount();firstIndex++) {
            int cmp = this.getKey(firstIndex).compareTo(leftKey);
            if (cmp >= 0)
                break;
        }

        List<TValue> retList = new ArrayList<TValue>();
        BTreeLeafNode<TKey,TValue> currLeaf=this;
        int currIndex=firstIndex;

        // case when all keys in the node are smaller than leftKey - shift to next rightSibling
        if (firstIndex>=this.getKeyCount()) {
            currLeaf = (BTreeLeafNode<TKey,TValue>) this.rightSibling;
            currIndex = 0;
        }

        while (currLeaf!=null && currLeaf.getKey(currIndex).compareTo(rightKey)<=0) {
            retList.addAll(currLeaf.getValueList(currIndex));
            currIndex++;
            if (currIndex>=currLeaf.getKeyCount()) {
                currLeaf = (BTreeLeafNode<TKey,TValue>) currLeaf.rightSibling;
                currIndex = 0;
            }
        }

        return retList;
    }

	protected void clearNode() {
		// counter updates
		for (TKey k : this.keys) {
			try {
				counter.countKeyRemoval(UtilGenerics.sizeOf(k.getClass()));
			} catch (UnsupportedGenericException e) {
				e.printStackTrace();
			}
		}

		// Todo fix this
//		for (byte[] val : this.values) {
//			counter.countValueRemoval(val.length);
//		}

		// clear node
		this.keys.clear();
		this.values.clear();
		this.keyCount=0;
	}



	public Object clone(BTreeNode oldNode) throws CloneNotSupportedException{
		BTreeLeafNode node = new BTreeLeafNode(ORDER, (BytesCounter) counter.clone());
		node.keyCount = keyCount;
		if (parentNode != null) {
			node.parentNode = oldNode;
		}
		if (leftSibling != null) {
			node.leftSibling = new BTreeLeafNode(leftSibling);
		}
		if (rightSibling != null) {
			node.rightSibling = new BTreeLeafNode(rightSibling);
		}
	/*	for (ArrayList list : values) {
			node.values.addAll(list);
		}*/

		node.keys.addAll(keys);

		return node;
	}

	public static Object deepClone(Object object) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			ObjectInputStream ois = new ObjectInputStream(bais);
			return ois.readObject();
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
