package com.github.dangxia.study.storm.state;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class OemDao {
	private JdbcTemplate realtimeJt;

	public Map<String, String> getOemidToSpid() {
		final Map<String, String> map = new HashMap<String, String>();
		String sql = "select oemid,spid from oem ";
		realtimeJt.query(sql, new RowMapper<Void>() {

			@Override
			public Void mapRow(ResultSet rs, int rowNum) throws SQLException {
				map.put(rs.getString("oemid"), rs.getString("spid"));
				return null;
			}
		});

		return map;
	}

	public JdbcTemplate getRealtimeJt() {
		return realtimeJt;
	}

	public void setRealtimeJt(JdbcTemplate realtimeJt) {
		this.realtimeJt = realtimeJt;
	}

}
