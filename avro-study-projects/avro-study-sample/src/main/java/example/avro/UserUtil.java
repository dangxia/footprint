/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package example.avro;

/**
 * @author XuehuiHe
 * @date 2014年7月8日
 */
public class UserUtil {
	private static String NAME = "Alyssa";
	private static Integer NUM = 256;
	private static String COLOR = "red";

	public static User createUserBySet() {
		User user1 = new User();
		user1.setName(NAME);
		user1.setFavoriteNumber(NUM);
		user1.setFavoriteColor(COLOR);
		return user1;
	}

	public static User createUserByConstructor() {
		return new User(NAME, NUM, COLOR);
	}

	public static User createUserByBuild() {
		return User.newBuilder().setName(NAME).setFavoriteColor(COLOR)
				.setFavoriteNumber(NUM).build();
	}

}
