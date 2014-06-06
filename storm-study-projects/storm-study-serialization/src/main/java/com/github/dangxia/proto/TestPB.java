/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package com.github.dangxia.proto;

import java.util.Map.Entry;

import com.github.dangxia.proto.PersonPB.Person;
import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * @author XuehuiHe
 * @date 2014年5月29日
 */
public class TestPB {
	public static void main(String[] args) {
		Person p = Person.newBuilder()// .setEmail("sldf")
				.setName("name").setId(1).build();

		for (Entry<FieldDescriptor, Object> entry : p.getAllFields().entrySet()) {
			FieldDescriptor descriptor = entry.getKey();
			String str = "index:" + descriptor.getIndex() + "\tfullname:"
					+ descriptor.getFullName() + "\tname:"
					+ descriptor.getName() + "\tnum:" + descriptor.getNumber();
			System.out.println(str + "\tvalue:" + entry.getValue().toString());
		}

		for (FieldDescriptor descriptor : Person.getDescriptor().getFields()) {
			String str = "index:" + descriptor.getIndex() + "\tfullname:"
					+ descriptor.getFullName() + "\tname:"
					+ descriptor.getName() + "\tnum:" + descriptor.getNumber();
			System.out.println(str + "\tvalue:");
		}
	}
}
