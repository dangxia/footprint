package com.github.dangxia;

import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;

import com.github.dangxia.util.ZkHolder;

public class TestZookeeper {
	public static void main(String[] args) throws InterruptedException {
		ZkHolder.get().create("/test/e/t-", "a",
				CreateMode.EPHEMERAL_SEQUENTIAL);
		TimeUnit.SECONDS.sleep(20);
		ZkHolder.get().close();

	}
}
