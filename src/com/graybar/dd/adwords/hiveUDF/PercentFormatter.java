package com.graybar.dd.adwords.hiveUDF;

import java.text.DecimalFormat;
import java.text.ParseException;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(
	name="PercentFromatter",
	value="Returns a correctly formatted double when given a string containing a percentage",
	extended="SELECT percentformatter('1.89%') FROM foo limit 1;"
	)

public class PercentFormatter extends UDF {
	
	public static double evaluate(String input) throws ParseException {
		try {
			if (input.startsWith("<") || input.startsWith(">")) {
				StringBuilder inputSB = new StringBuilder(input);
				input = inputSB.substring(2);
			}
			DecimalFormat myFormatter = new DecimalFormat("0.0#%");
			double output = (myFormatter.parse(input)).doubleValue();
			return output;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
}
