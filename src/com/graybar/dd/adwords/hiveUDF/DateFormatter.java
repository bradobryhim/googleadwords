package com.graybar.dd.adwords.hiveUDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
	name="DateFormatter",
	value="Returns a date formatted YYYY-MM-DD as specificed by Hive date type.",
	extended="SELECT DateFormatter('1/2/2016') FROM foo limit 1;"
	)

public class DateFormatter extends UDF {
	
	public static String evaluate (String input) throws ParseException {
		try {
			SimpleDateFormat oldFormat = new SimpleDateFormat("M/d/yyyy");
			SimpleDateFormat newFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date Date = oldFormat.parse(input);
			String output = newFormat.format(Date);
			return output;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
