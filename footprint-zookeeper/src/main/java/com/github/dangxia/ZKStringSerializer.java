package com.github.dangxia;

import java.io.UnsupportedEncodingException;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class ZKStringSerializer implements ZkSerializer {

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
