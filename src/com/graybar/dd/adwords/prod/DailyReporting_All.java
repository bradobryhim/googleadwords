package com.graybar.dd.adwords.prod;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;

import org.apache.hadoop.fs.Path;
import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.client.reporting.ReportingConfiguration;
import com.google.api.ads.adwords.lib.jaxb.v201607.DownloadFormat;
import com.google.api.ads.adwords.lib.jaxb.v201607.ReportDefinition;
import com.google.api.ads.adwords.lib.jaxb.v201607.ReportDefinitionDateRangeType;
import com.google.api.ads.adwords.lib.jaxb.v201607.ReportDefinitionReportType;
import com.google.api.ads.adwords.lib.jaxb.v201607.Selector;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponse;
import com.google.api.ads.adwords.lib.utils.ReportDownloadResponseException;
import com.google.api.ads.adwords.lib.utils.v201607.ReportDownloader;
import com.google.api.ads.common.lib.auth.OfflineCredentials;
import com.google.api.ads.common.lib.auth.OfflineCredentials.Api;
import com.google.api.ads.common.lib.conf.ConfigurationLoadException;
import com.google.api.ads.common.lib.exception.OAuthException;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.api.client.auth.oauth2.Credential;
import com.google.common.collect.Lists;

public class DailyReporting_All {
	/*
	 * The DailyReporting class sends daily updates to HDFS from Google AdWords 
	 * in the production Azure cluster. Every Google report except Click_Performance is 
	 * updated through this class. Click Performance is a daily report that has enough 
	 * different variables & requirements that it necessitated its own separate code. 
	 * 
	 * Due to Google AdWords click fraud prevention, this program accounts for deltas 
	 * by doing different things depending on what day of the month it is.
	 * 
	 * Days 1-6:
	 * 		Download & overwrite the current-month-to-date CSV file for each report
	 * 		Download & overwrite the last month's CSV file for each report
	 * Day 7:
	 * 		Append the last month's data for each report to the historical record CSV
	 * 		Delete the last month's CSV file
	 * 		Download & overwrite the current-month-to-date CSV file for each report
	 * Days 8-EoM:
	 * 		Download & overwrite the current-month-to-date CSV file for each report
	 */
	
//	This path is for for Sandbox01
//	public static final String LZ_PATH = "hdfs://cslhdpsbx01.graybar.com/data_LZ/api_integration/google_adwords/";
//	This path is for Azure Cluster:
//	public static final String LZ_PATH = "hdfs://cplhdpmp01.graybar.com/data_LZ/Digital_Business/API_Integration/Google_AdWords/";
//  This path for on-prem:
	public static final String LZ_PATH = "hdfs://splhdpmp01.graybar.com/data_LZ/Digital_Business/API_Integration/Google_AdWords/";
	
	public static void main(String[] args) throws Exception {
		
		ArrayList<String> CustomerIDList = new ArrayList<String>(); 
        CustomerIDList.addAll(Lists.newArrayList(
        		"3107410061","6071514471","8031908053","8445299289","9417573739","9521584010"
        		));
        
        ArrayList<String> ReportList = new ArrayList<String>();
        ReportList.addAll(Lists.newArrayList(
        		"ACCOUNT_PERFORMANCE_REPORT",
        		"ADGROUP_PERFORMANCE_REPORT",
                "AD_PERFORMANCE_REPORT",
                "AUDIENCE_PERFORMANCE_REPORT",
                "AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT",
                "CAMPAIGN_LOCATION_TARGET_REPORT",
                "CAMPAIGN_PERFORMANCE_REPORT",
//                "CLICK_PERFORMANCE_REPORT",
                "FINAL_URL_REPORT",
                "GEO_PERFORMANCE_REPORT",
                "KEYWORDLESS_CATEGORY_REPORT",
                "KEYWORDLESS_QUERY_REPORT",
                "KEYWORDS_PERFORMANCE_REPORT",
                "PAID_ORGANIC_QUERY_REPORT",
                "PLACEMENT_PERFORMANCE_REPORT",
                "PRODUCT_PARTITION_REPORT",
                "SEARCH_QUERY_PERFORMANCE_REPORT",
                "SHARED_SET_REPORT",
                "SHARED_SET_CRITERIA_REPORT",
                "SHOPPING_PERFORMANCE_REPORT",
                "URL_PERFORMANCE_REPORT",
                "USER_AD_DISTANCE_REPORT"
                ));
        
//        These functions create a calendar object to obtain the current day of the month, as well
//        as strings for the previous and current month's abbreviations.
        Calendar cal= Calendar.getInstance();
        int currentDay = cal.get(Calendar.DAY_OF_MONTH);
        String currentMonth = cal.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.US);
        cal.add(Calendar.MONTH, -1);
        String lastMonth = cal.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.US);
        
//        If the current day of the month is less than the 7th, download & overwrite the previous 
//        & current monthly reports.
        if (currentDay < 7) {
        	Credential oAuth2Credential = GetCredentials();
        	for (int i=0; i < CustomerIDList.size(); i++) {
        		AdWordsSession session = GetSession(CustomerIDList.get(i), oAuth2Credential);
        		for (int j=0; j < ReportList.size(); j++) {        			
        			Path outputPathCurrent = new Path(LZ_PATH +ReportList.get(j) +"/" +ReportList.get(j) +"_" +CustomerIDList.get(i) +"_" + currentMonth +".csv");
        			Path outputPathLast = new Path(LZ_PATH +ReportList.get(j) +"/" +ReportList.get(j) +"_" +CustomerIDList.get(i) +"_" + lastMonth +".csv");
        			
        			InputStream inputStream = runReport(session, ReportList.get(j), ReportDefinitionDateRangeType.THIS_MONTH, false);
        			HDFSwriter.streamCreate(inputStream, outputPathCurrent);
        			InputStream inputStream2 = runReport(session, ReportList.get(j), ReportDefinitionDateRangeType.LAST_MONTH, false);
        			HDFSwriter.streamCreate(inputStream2, outputPathLast);
        		}
        	}
        }
//        If the current day of the month is the 7th, download & append the previous months's report to the 
//        historical data, delete the previous month's report, and download & overwrite the current monthly
//        report.
        else if (currentDay == 7) {
        	Credential oAuth2Credential = GetCredentials();
        	for (int i=0; i < CustomerIDList.size(); i++) {
        		AdWordsSession session = GetSession(CustomerIDList.get(i), oAuth2Credential);
        		for (int j=0; j < ReportList.size(); j++) {        			
        			Path outputPathAppend = new Path(LZ_PATH +ReportList.get(j) +"/" +ReportList.get(j) +"_" +CustomerIDList.get(i) +".csv");
        			Path deletePath = new Path(LZ_PATH +ReportList.get(j) +"/" +ReportList.get(j) +"_" +CustomerIDList.get(i) +"_" + lastMonth +".csv");
        			Path outputPathCurrent = new Path(LZ_PATH +ReportList.get(j) +"/" +ReportList.get(j) +"_" +CustomerIDList.get(i) +"_" + currentMonth +".csv");
        			
        			InputStream inputStream = runReport(session, ReportList.get(j), ReportDefinitionDateRangeType.LAST_MONTH, true);
        			HDFSwriter.streamAppend(inputStream, outputPathAppend);
        			HDFSwriter.fileDelete(deletePath);
        			InputStream inputStream2 = runReport(session, ReportList.get(j), ReportDefinitionDateRangeType.THIS_MONTH, false);
        			HDFSwriter.streamCreate(inputStream2, outputPathCurrent);
        		}
        	}
        }
//        If the current day of the month is greater than the 7th, download & overwrite the current monthly
//        report.
        else if (currentDay > 7) {
        	Credential oAuth2Credential = GetCredentials();
        	for (int i=0; i < CustomerIDList.size(); i++) {
        		AdWordsSession session = GetSession(CustomerIDList.get(i), oAuth2Credential);
        		for (int j=0; j < ReportList.size(); j++) {        			
        			Path outputPathCurrent = new Path(LZ_PATH +ReportList.get(j) +"/" +ReportList.get(j) +"_" +CustomerIDList.get(i) +"_" + currentMonth +".csv");
        			
        			InputStream inputStream = runReport(session, ReportList.get(j), ReportDefinitionDateRangeType.THIS_MONTH, false);
        			HDFSwriter.streamCreate(inputStream, outputPathCurrent);
        		}
        	}
        }
	}
	public static Credential GetCredentials() {
		Credential oAuth2Credential;
		try {
			oAuth2Credential = new OfflineCredentials.Builder()
						.forApi(Api.ADWORDS)
				        .fromFile()
				        .build()
				        .generateCredential();
			return oAuth2Credential;
		} catch (OAuthException | ValidationException | ConfigurationLoadException e) {
			System.out.println("Unable to build oAuth2Credential. ");
			e.printStackTrace();
		}
		return null;
	}
	public static AdWordsSession GetSession(String CustomerID, Credential oAuth2Credential) {
		try {
			AdWordsSession session = new AdWordsSession.Builder()
					.fromFile()
					.withClientCustomerId(CustomerID)
					.withOAuth2Credential(oAuth2Credential)
					.build();
			return session;
		} catch (ValidationException | ConfigurationLoadException e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to build AdWords session. ");
			e.printStackTrace();
		}
		return null;
	}
	public static InputStream runReport(AdWordsSession session, String ReportName, ReportDefinitionDateRangeType DateRangeType, Boolean ColHeader) throws Exception {
	    // Create selector.  ReportBuilder function can take a DateMin and DateMax, however this variables
		// are commented out for the daily report build.		
	    Selector selector = ReportBuilder.ReportBuilder(ReportName, null, null);

	    // Create report definition. DateRangeType is passed to the function.
	    ReportDefinition reportDefinition = new ReportDefinition();
	    reportDefinition.setReportName(ReportName + System.currentTimeMillis());
	    reportDefinition.setDateRangeType(DateRangeType);
	    reportDefinition.setReportType(ReportDefinitionReportType.fromValue(ReportName));
	    reportDefinition.setDownloadFormat(DownloadFormat.CSV);
	    
	    /* 
	     * Header and report summary is skipped for data import.  Column header is added to the current and 
	     * last month reports, but skipped for day seven appends to historical data.  Where it doesn't impact
	     * performance, zero impressions are included in the reports. However, Adgroup Performance and Keyword
	     * Performance reports have too much data, so zero impressions are also skipped, despite their 
	     * support.
	    */	    
	    ArrayList<String> zeroImpressionSupport = new ArrayList<String>(); 
        zeroImpressionSupport.addAll(Lists.newArrayList(
        		"ACCOUNT_PERFORMANCE_REPORT",
//        		"ADGROUP_PERFORMANCE_REPORT",
        		"AUDIENCE_PERFORMANCE_REPORT",
        		"CAMPAIGN_LOCATION_TARGET_REPORT",
        		"CAMPAIGN_PERFORMANCE_REPORT",
//        		"KEYWORDS_PERFORMANCE_REPORT",
        		"PLACEMENT_PERFORMANCE_REPORT",
        		"PRODUCT_PARTITION_REPORT"));
        
        ReportingConfiguration reportingConfiguration = null;
        
        if (zeroImpressionSupport.contains(ReportName)) {
		    reportingConfiguration =
			        new ReportingConfiguration.Builder()
			            .skipReportHeader(true)
			            .skipReportSummary(true)
			            .skipColumnHeader(ColHeader)
			            // Enable to allow rows with zero impressions to show.
			            .includeZeroImpressions(true)
			            .build();
        }
        else {
		    reportingConfiguration =
			        new ReportingConfiguration.Builder()
			            .skipReportHeader(true)
			            .skipReportSummary(true)
			            .skipColumnHeader(ColHeader)
			            // Enable to allow rows with zero impressions to show.
			            .includeZeroImpressions(false)
			            .build();
        }
		    
	    session.setReportingConfiguration(reportingConfiguration);
	    
	    reportDefinition.setSelector(selector);

	    try {
	      // Set the property api.adwords.reportDownloadTimeout or call
	      // ReportDownloader.setReportDownloadTimeout to set a timeout (in milliseconds)
	      // for CONNECT and READ in report downloads.
	      ReportDownloadResponse response =
	          new ReportDownloader(session).downloadReport(reportDefinition);
	      
	      return response.getInputStream();
	      
	    } catch (ReportDownloadResponseException e) {
	      System.out.printf("Report was not downloaded due to: %s%n", e);
	    }
		return null;
	  }
}
