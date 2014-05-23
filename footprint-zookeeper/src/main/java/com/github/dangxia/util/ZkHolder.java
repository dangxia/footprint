package com.github.dangxia.util;

import java.io.UnsupportedEncodingException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class ZkHolder {

	private static class Holder {
		private static ZkClient holder = initZk();
	}

	private static ZkClient initZk() {
		ZkClient client = new ZkClient("data-slave3.voole.com:2181");
		client.setZkSerializer(new ZKStringSerializer());
		return client;
	}

	private ZkHolder() {
	}

	public static ZkClient get() {
		return Holder.holder;
	}

	public static class ZKStringSerializer implements ZkSerializer {

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			if (data != null) {
				try {
					return data.toString().getBytes("UTF-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
			return null;
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if (bytes != null) {
				try {
					return new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
			return null;
		}
	}

}
