/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package example.avro;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author XuehuiHe
 * @date 2014年7月8日
 */
public class CompareUserCreator {

	@Test
	public void test() {
		User user1 = UserUtil.createUserBySet();
		User user2 = UserUtil.createUserByConstructor();
		User user3 = UserUtil.createUserByBuild();

		Assert.assertEquals(user1, user2);
		Assert.assertEquals(user1, user3);

		Assert.assertEquals(user1.hashCode(), user2.hashCode());
		Assert.assertEquals(user1.hashCode(), user3.hashCode());
	}
}
