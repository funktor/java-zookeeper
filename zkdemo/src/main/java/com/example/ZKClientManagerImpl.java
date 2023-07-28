package com.example;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKClientManagerImpl implements ZKManager {
    private static ZooKeeper zkeeper;
	private static ZKConnection zkConnection;

	public ZKClientManagerImpl() {
		initialize();
	}

	/**
	 * Initialize connection
	 */
	private void initialize() {
		try {
			zkConnection = new ZKConnection();
			zkeeper = zkConnection.connect("localhost");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	/**
	 * Close the zookeeper connection
	 */
	public void closeConnection() {
		try {
			zkConnection.close();
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Override
	public void create(String path, byte[] data, boolean isPersistent, boolean isSequential) throws KeeperException,
			InterruptedException {
        
        Stat stat = getZNodeStats(path);

        if (stat == null) {
            if (isPersistent) {
                zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				    CreateMode.PERSISTENT);
            }
            else {
                if (isSequential) {
                    zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				        CreateMode.EPHEMERAL_SEQUENTIAL);
                }
                else {
                    zkeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				        CreateMode.EPHEMERAL);
                }
            }
        }
	}

	@Override
	public Stat getZNodeStats(String path) throws KeeperException,
			InterruptedException {
		Stat stat = zkeeper.exists(path, true);
		if (stat != null) {
			System.out.println("Node exists and the node version is "
					+ stat.getVersion());
		} else {
			System.out.println("Node does not exists");
		}
		return stat;
	}

	@Override
	public String getZNodeData(String path, boolean watchFlag) throws KeeperException,
			InterruptedException {
		try {
			Stat stat = getZNodeStats(path);
			byte[] b = null;

			if (stat != null) {
				if(watchFlag){
					ZKWatcher watch = new ZKWatcher();
					b = zkeeper.getData(path, watch,null);
					watch.await();
				}else{
					b = zkeeper.getData(path, null, null);
				}

				String data = new String(b, "UTF-8");
				System.out.println(data);
				
				return data;
			} else {
				System.out.println("Node does not exists");
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return null;
	}

	@Override
	public void update(String path, byte[] data) throws KeeperException,
			InterruptedException {
		int version = zkeeper.exists(path, true).getVersion();
		zkeeper.setData(path, data, version);
    }

	@Override
	public List<String> getZNodeChildren(String path) throws KeeperException,
			InterruptedException {
		Stat stat = getZNodeStats(path);
		List<String> children  = null;

		if (stat != null) {
			children = zkeeper.getChildren(path, false);
			for (int i = 0; i < children.size(); i++)
				System.out.println(children.get(i)); 
		} else {
			System.out.println("Node does not exists");
		}
		return children;
	}

	@Override
	public void delete(String path) throws KeeperException,
			InterruptedException {
		int version = zkeeper.exists(path, true).getVersion();
		zkeeper.delete(path, version);
	}
}
