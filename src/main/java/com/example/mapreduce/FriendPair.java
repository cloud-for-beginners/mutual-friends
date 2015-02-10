/*
 * (c) Cloud for Beginners.
 * 
 * author: tmusabbir
 */
package com.example.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * The Class FriendPair.
 */
public class FriendPair implements WritableComparable<FriendPair> {

	/** The first friend. */
	private Text firstFriend;

	/** The second friend. */
	private Text secondFriend;


	/**
	 * Instantiates a new friend pair.
	 */
	public FriendPair() {
		firstFriend = new Text();
		secondFriend = new Text();
	}


	/**
	 * Instantiates a new friend pair.
	 * 
	 * @param first the first
	 * @param second the second
	 */
	public FriendPair(Text first, Text second) {
		if (first.compareTo(second) > 0) {
			this.firstFriend = second;
			this.secondFriend = first;
		} else {
			this.firstFriend = first;
			this.secondFriend = second;
		}
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof FriendPair) {
			FriendPair fp = (FriendPair) obj;
			return firstFriend.equals(fp.firstFriend) && secondFriend.equals(fp.secondFriend);
		}
		return false;
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		firstFriend.write(out);
		secondFriend.write(out);
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		firstFriend.readFields(in);
		secondFriend.readFields(in);
	}


	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(FriendPair fp) {
		if (firstFriend.compareTo(fp.firstFriend) == 0) {
			return secondFriend.compareTo(fp.secondFriend);
		} else {
			return firstFriend.compareTo(fp.firstFriend);
		}
	}


	/**
	 * Gets the first friend.
	 * 
	 * @return the first friend
	 */
	public Text getFirstFriend() {
		return firstFriend;
	}


	/**
	 * Sets the first friend.
	 * 
	 * @param firstFriend the new first friend
	 */
	public void setFirstFriend(Text firstFriend) {
		this.firstFriend = firstFriend;
	}


	/**
	 * Gets the second friend.
	 * 
	 * @return the second friend
	 */
	public Text getSecondFriend() {
		return secondFriend;
	}


	/**
	 * Sets the second friend.
	 * 
	 * @param secondFriend the new second friend
	 */
	public void setSecondFriend(Text secondFriend) {
		this.secondFriend = secondFriend;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String val = "(" + firstFriend + " " + secondFriend + ")";
		return val;
	}
}
