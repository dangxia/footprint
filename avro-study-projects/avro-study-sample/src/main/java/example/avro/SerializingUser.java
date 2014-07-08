/*
 * Copyright (C) 2014 BEIJING UNION VOOLE TECHNOLOGY CO., LTD
 */
package example.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * @author XuehuiHe
 * @date 2014年7月8日
 */
public class SerializingUser {
	public static void main(String[] args) throws IOException {
		User user1 = UserUtil.createUserBySet();
		User user2 = UserUtil.createUserByConstructor();
		user2.setFavoriteColor("sjdlfjsljdlfjsldjlfj");
		User user3 = UserUtil.createUserByBuild();
		user3.setName("jsdlfjlsjdlfjlsjdlfjsdfsdfsdfsdfsdfs");

		File f = new File("users.avro");
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(
				User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(
				userDatumWriter);
		dataFileWriter.create(user1.getSchema(), f);
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);
		dataFileWriter.append(user3);
		dataFileWriter.close();

	}

	public static void main2(String[] args) throws IOException {
		File file = new File("users.avro");
		DatumReader<User> userDatumReader = new SpecificDatumReader<User>(
				User.class);
		DataFileReader<User> dataFileReader = new DataFileReader<User>(file,
				userDatumReader);
		User user = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			user = dataFileReader.next(user);
			System.out.println(user);
		}
	}
}
