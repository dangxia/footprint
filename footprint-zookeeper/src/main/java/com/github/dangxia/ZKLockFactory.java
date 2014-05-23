package com.github.dangxia;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import com.github.dangxia.util.ZkHolder;

public class ZKLockFactory {
	private final String path;
	private final String prefix;
	private final ZkClient client;

	public ZKLockFactory(String path, String prefix) {
		this.path = path;
		this.prefix = prefix;
		this.client = ZkHolder.get();
		initDir();
	}

	private void initDir() {
		try {
			if (!getClient().exists(getPath())) {
				getClient().createPersistent(getPath());
			}
		} catch (ZkNodeExistsException e) {
		}
	}

	public ZKLockFactory(String path) {
		this(path, "lock-");
	}

	public ZkLock newLock() {
		return new ZkLock();
	}

	public class ZkLock {
		private String lockPath;
		private int index;
		private int preIndex = -1;

		private ZkLock() {
		}

		public void lock() {
			String lockPath = getClient().createEphemeralSequential(
					getAbsolutePath(), null);
			this.lockPath = lockPath;
			this.index = getIndex(lockPath);

			getClient().subscribeChildChanges(getPath(),
					new IZkChildListener() {

						@Override
						public void handleChildChange(String parentPath,
								List<String> currentChilds) throws Exception {
							preIndex = getPreIndex(currentChilds, index);
						}
					});
		}
	}

	public int getPreIndex(List<String> currentChilds, int index) {
		// TODO
		return 0;
	}

	public int getIndex(String lockPath) {
		String index = lockPath.replace(getPrefix(), "");
		return Integer.parseInt(index);
	}

	public String getPath() {
		return path;
	}

	public String getPrefix() {
		return prefix;
	}

	public ZkClient getClient() {
		return client;
	}

	public String getAbsolutePath() {
		return getPath() + "/" + getPrefix();
	}

}
