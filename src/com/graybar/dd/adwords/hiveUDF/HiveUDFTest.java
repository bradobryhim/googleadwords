package com.graybar.dd.adwords.hiveUDF;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;

import org.junit.Test;

public class HiveUDFTest {

	@Test
	public void testPercent() throws ParseException {
		assertEquals(PercentFormatter.evaluate("1.89%"), 0.0189, 0.0);
		assertEquals(PercentFormatter.evaluate("15.0%"), 0.150, 0.0);
		assertEquals(PercentFormatter.evaluate("97.98%"), 0.97980, 0.0);
		assertEquals(PercentFormatter.evaluate("0.88%"), 0.0088, 0.0);
		assertEquals(PercentFormatter.evaluate("0.00%"), 0, 0.0);
		assertEquals(PercentFormatter.evaluate("< 10%"), 0.10, 0.0);
		assertEquals(PercentFormatter.evaluate("> 90%"), 0.90, 0.0);
	}
	
	@Test
	public void testDate() throws ParseException {
		assertEquals("2016-02-01", DateFormatter.evaluate("2/1/2016"));
		assertEquals("2015-12-01", DateFormatter.evaluate("12/1/2015"));
	}

}
