package com.example;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public interface  ZKManager {
    /**
	 * Create a Znode and save some data
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void create(String path, byte[] data, boolean isPersistent) throws KeeperException, InterruptedException;

	/**
	 * Get the ZNode Stats
	 * 
	 * @param path
	 * @return Stat
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Stat getZNodeStats(String path) throws KeeperException,
			InterruptedException;

	/**
	 * Get ZNode Data
	 * 
	 * @param path
	 * @param boolean watchFlag
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Object getZNodeData(String path,boolean watchFlag) throws KeeperException,
			InterruptedException;

	/**
	 * Update the ZNode Data
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void update(String path, byte[] data) throws KeeperException,
			InterruptedException;

	/**
	 * Get ZNode children
	 * 
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 * return List
	 */
	public List<String> getZNodeChildren(String path) throws KeeperException,
			InterruptedException;

	/**
	 * Delete the znode
	 * 
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void delete(String path) throws KeeperException,
			InterruptedException;
}
