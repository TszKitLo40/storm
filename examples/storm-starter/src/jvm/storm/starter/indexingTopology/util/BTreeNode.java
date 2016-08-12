package storm.starter.indexingTopology.util;

import storm.starter.indexingTopology.Config.Config;
import storm.starter.indexingTopology.exception.UnsupportedGenericException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

enum TreeNodeType {
	InnerNode,
	LeafNode
}

abstract class BTreeNode<TKey extends Comparable<TKey>> implements Serializable{
	protected final int ORDER;
 //   protected final BytesCounter counter;
	protected BytesCounter counter;
	protected ArrayList<TKey> keys;
	protected int keyCount;
	protected BTreeNode<TKey> parentNode;
	protected BTreeNode<TKey> leftSibling;
	protected BTreeNode<TKey> rightSibling;
	protected ReadWriteLock rwl;
	protected Lock readLock;
	protected Lock writeLock;

    protected BTreeNode(int order, BytesCounter counter) {
        this.keyCount = 0;
        ORDER = order;
        this.parentNode = null;
        this.leftSibling = null;
        this.rightSibling = null;
        this.counter=counter;
        this.counter.countNewNode();
		this.rwl = new ReentrantReadWriteLock();
		this.readLock = rwl.readLock();
		this.writeLock = rwl.writeLock();
    }


	public int getKeyCount() {
		return this.keyCount;
	}

	@SuppressWarnings("unchecked")
	public TKey getKey(int index) {
		return (TKey)this.keys.get(index);
	}

	public void setKey(int index, TKey key) throws UnsupportedGenericException {
        if (index<this.keys.size())
		    this.keys.set(index,key);
        else if (index==this.keys.size()) {
            this.counter.countKeyAddition(UtilGenerics.sizeOf(key.getClass()));
            this.keys.add(index, key);
			keyCount += 1;
        }
        else {
            throw new ArrayIndexOutOfBoundsException("index is out of bounds");
        }
	}

	public BTreeNode<TKey> getParent() {
		return this.parentNode;
	}

	public void setParent(BTreeNode<TKey> parent) {
		this.parentNode = parent;
	}	
	
	public abstract TreeNodeType getNodeType();
	
	
	/**
	 * Search a key on current node, if found the key then return its position,
	 * otherwise return -1 for a leaf node, 
	 * return the child node index which should contain the key for a internal node.
	 */
	public abstract int search(TKey key);

    public abstract Collection<BTreeNode<TKey>> recursiveSerialize(ByteBuffer allocatedBuffer);
	
	/* The codes below are used to support insertion operation */
	
	public boolean isOverflow() {
		return this.getKeyCount() > this.ORDER;
	}

	public boolean willOverflowOnInsert(TKey key) {
		for (TKey k : this.keys) {
			if (k.compareTo(key)==0)
				return false;
		}

		return this.getKeyCount() == this.ORDER;
	}

	public BTreeNode<TKey> dealOverflow(SplitCounterModule sm, BTreeLeafNode leaf) {
		int midIndex = this.getKeyCount() / 2;
		TKey upKey = this.getKey(midIndex);

		sm.addCounter();
		BTreeNode<TKey> newRNode = this.split();
		if (this.getParent() == null) {
			this.setParent(new BTreeInnerNode<TKey>(this.ORDER,this.counter));
            counter.increaseHeightCount();
		}
		newRNode.setParent(this.getParent());
		
		// maintain links of sibling nodes
		newRNode.setLeftSibling(this);
		newRNode.setRightSibling(this.rightSibling);
		if (this.getRightSibling() != null)
			this.getRightSibling().setLeftSibling(newRNode);
		this.setRightSibling(newRNode);
		// push up a key to parent internal node

//		synchronized (this.getParent()) {
			if (this.getParent() == null) {
				System.out.println("parent is null");
			}
			return this.getParent().pushUpKey(upKey, this, newRNode, sm, leaf);
//		}

	}
	
	protected abstract BTreeNode<TKey> split();
	
	protected abstract BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode, SplitCounterModule sm, BTreeLeafNode leaf);
	
	
	
	
	
	
	/* The codes below are used to support deletion operation */
	
	public boolean isUnderflow() {
		return this.getKeyCount() < ((this.ORDER+1) / 2);
	}
	
	public boolean canLendAKey() {
		return this.getKeyCount() > ((this.ORDER+1) / 2);
	}
	
	public BTreeNode<TKey> getLeftSibling() {
		if (this.leftSibling != null && this.leftSibling.getParent() == this.getParent())
			return this.leftSibling;
		return null;
	}

	public void setLeftSibling(BTreeNode<TKey> sibling) {
		this.leftSibling = sibling;
	}

	public BTreeNode<TKey> getRightSibling() {
		if (this.rightSibling != null && this.rightSibling.getParent() == this.getParent())
			return this.rightSibling;
		return null;
	}

	public void setRightSibling(BTreeNode<TKey> sibling) {
		this.rightSibling = sibling;
	}

	public BTreeNode<TKey> dealUnderflow() {
		if (this.getParent() == null)
			return null;
		
		// try to borrow a key from sibling
		BTreeNode<TKey> leftSibling = this.getLeftSibling();
		if (leftSibling != null && leftSibling.canLendAKey()) {
			this.getParent().processChildrenTransfer(this, leftSibling, leftSibling.getKeyCount() - 1);
			return null;
		}
		
		BTreeNode<TKey> rightSibling = this.getRightSibling();
		if (rightSibling != null && rightSibling.canLendAKey()) {
			this.getParent().processChildrenTransfer(this, rightSibling, 0);
			return null;
		}
		
		// Can not borrow a key from any sibling, then do fusion with sibling
		if (leftSibling != null) {
			return this.getParent().processChildrenFusion(leftSibling, this);
		}
		else {
			return this.getParent().processChildrenFusion(this, rightSibling);
		}
	}
	
	protected abstract void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex);
	
	protected abstract BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild);
	
	protected abstract void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling);
	
	protected abstract TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex);

	public void print() {
		for (TKey k : keys)
			System.out.print(k+" ");
		System.out.println();
	}

	public boolean isOverflowIntemplate() {
		double threshold = this.ORDER * Config.templateOverflowPercentage;
		return ((double) this.getKeyCount() > threshold);
	}

	public abstract Object clone(BTreeNode oldNode) throws CloneNotSupportedException;


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