package com.graybar.dd.adwords.prod;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.fs.Path;

import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.client.reporting.ReportingConfiguration;
import com.google.api.ads.adwords.lib.jaxb.v201607.DateRange;
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

public class DailyReporting_ClickPerformance {
	
//	This path is for for Sandbox01
//	public static final String LZ_PATH = "hdfs://cslhdpsbx01.graybar.com/data_LZ/api_integration/google_adwords/CLICK_PERFORMANCE_REPORT/";
//	This path is for Azure Cluster:
//	public static final String LZ_PATH = "hdfs://cplhdpmp01.graybar.com/data_LZ/Digital_Business/API_Integration/Google_AdWords/CLICK_PERFORMANCE_REPORT/";
//  This path for on-prem:
	public static final String LZ_PATH = "hdfs://splhdpmp01.graybar.com/data_LZ/Digital_Business/API_Integration/Google_AdWords/CLICK_PERFORMANCE_REPORT/";
	
	public static void main(String[] args) throws Exception {
		ArrayList<String> CustomerIDList = new ArrayList<String>(); 
		CustomerIDList.addAll(Lists.newArrayList(
				"3107410061","6071514471","8031908053","8445299289","9417573739","9521584010"
				));

		// These functions create a calendar object to obtain the current day of the month, as well
		// as strings for the previous and current month's abbreviations.

		Calendar cal= Calendar.getInstance();
		int currentDayInt = cal.get(Calendar.DAY_OF_MONTH);
		String currentMonthShort = cal.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.US);
		int currentMonthInt = cal.get(Calendar.MONTH);
		// Months are zero-indexed, i.e. January = 0.
		currentMonthInt += 1;
		String currentYearString = Integer.toString(cal.get(Calendar.YEAR));
		String currentMonthFirstDay = "01";
		String currentMonthSecondDay = "02";
		String currentMonthString = null;
		if (currentMonthInt < 10) {
			currentMonthString = "0" + currentMonthInt;
		}
		else {
			currentMonthString = Integer.toString(currentMonthInt);
		}
		String currentDayString = null;
		if (currentDayInt < 10) {
			currentDayString = "0" + Integer.toString(currentDayInt);
		}
		else {
			currentDayString = Integer.toString(currentDayInt);
		}
		// Rolls the calendar back one month to get numeric date strings and abbreviation strings.
		cal.add(Calendar.MONTH, -1);
		String lastMonthShort = cal.getDisplayName(Calendar.MONTH, Calendar.SHORT, Locale.US);
		int lastMonthInt = cal.get(Calendar.MONTH);
		lastMonthInt += 1;
		String lastMonthString = null;
		if (lastMonthInt < 10) {
			lastMonthString = "0" + lastMonthInt;
		}
		else {
			lastMonthString = Integer.toString(lastMonthInt);
		}
		String lastYearString = Integer.toString(cal.get(Calendar.YEAR));
		String lastMonthFirstDay = "01";
		String lastMonthSecondDay = "02";
		String lastMonthLastDay = Integer.toString(cal.getMaximum(Calendar.DAY_OF_MONTH));

		String lastMonthDay1 = lastYearString + lastMonthString + lastMonthFirstDay;
		String lastMonthDay2 = lastYearString + lastMonthString + lastMonthSecondDay;
		String lastMonthEoM = lastYearString + lastMonthString + lastMonthLastDay;

		String currentMonthDay1 = currentYearString + currentMonthString + currentMonthFirstDay;
		String currentMonthDay2 = currentYearString + currentMonthString + currentMonthSecondDay;
		String currentMonthEoM = currentYearString + currentMonthString + currentDayString;

		 /*Creates a SimpleDateFormat in the format the AdWords accepts, then creates dates
		 for first, second and end of month for last month and the current month. These Date objects
		 will be used in a for-loop to iterate over the calendar days for each report request.
		*/
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		Date lastMonthDay1Date = formatter.parse(lastMonthDay1);
		Date lastMonthDay2Date = formatter.parse(lastMonthDay2);
		Date lastMonthEndDate = formatter.parse(lastMonthEoM);
		LocalDate lastMonthDay1LD = lastMonthDay1Date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		LocalDate lastMonthDay2LD = lastMonthDay2Date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		LocalDate lastMonthEndLD = lastMonthEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		Date currentMonthDay2Date = formatter.parse(currentMonthDay2);
		Date currentMonthEndDate = formatter.parse(currentMonthEoM);
		LocalDate currentMonthDay2LD = currentMonthDay2Date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		LocalDate currentMonthEndLD = currentMonthEndDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

		if (currentDayInt < 7) {
			Credential oAuth2Credential = GetCredentials();
			for (int i=0; i < CustomerIDList.size(); i++) {
				AdWordsSession session = GetSession(CustomerIDList.get(i), oAuth2Credential);
				// Set path for the last month's report and current month's report:
				Path outputPathLast = new Path(LZ_PATH +"CLICK_PERFORMANCE_REPORT_" +CustomerIDList.get(i) +"_" + lastMonthShort +".csv");
				Path outputPathCurrent = new Path(LZ_PATH +"CLICK_PERFORMANCE_REPORT_" +CustomerIDList.get(i) +"_" + currentMonthShort +".csv");

				// Get a report for the first day of last month and create(overwrite) it in HDFS
				InputStream inputStream = runReport(session, "CLICK_PERFORMANCE_REPORT", ReportDefinitionDateRangeType.CUSTOM_DATE, lastMonthDay1, lastMonthDay1);
				HDFSwriter.streamCreate(inputStream, outputPathLast);
				// For days 2 through EoM for last month, get the report and append it to last month's report.
				for (LocalDate date = lastMonthDay2LD; date.isBefore(lastMonthEndLD); date = date.plusDays(1)) {
					InputStream inputStream2 = runReport(session, "CLICK_PERFORMANCE_REPORT", ReportDefinitionDateRangeType.CUSTOM_DATE, date.toString(), date.toString());
					HDFSwriter.streamAppend(inputStream2, outputPathLast);
				}
				// Get a report for the first day of the current month and create(overwrite) it in HDFS
				InputStream inputStream3 = runReport(session, "CLICK_PERFORMANCE_REPORT", ReportDefinitionDateRangeType.CUSTOM_DATE, currentMonthDay1, currentMonthDay1);
				HDFSwriter.streamCreate(inputStream3, outputPathCurrent);
				// For days 2 through the current day of the month, get the report and append it to the current month's report.
				for (LocalDate date = currentMonthDay2LD; date.isBefore(currentMonthEndLD); date = date.plusDays(1)) {
					InputStream inputStream4 = runReport(session, "CLICK_PERFORMANCE_REPORT", ReportDefinitionDateRangeType.CUSTOM_DATE, date.toString(), date.toString());
					HDFSwriter.streamAppend(inputStream4, outputPathCurrent);
				}
			}            	
		}
		if (currentDayInt == 7) {
			Credential oAuth2Credential = GetCredentials();
			for (int i=0; i < CustomerIDList.size(); i++) {
				AdWordsSession session = GetSession(CustomerIDList.get(i), oAuth2Credential);
				// Set path to delete last month's report, append last month to historical record, and update current month's report.
				Path outputPathDelete = new Path(LZ_PATH +"CLICK_PERFORMANCE_REPORT_" +CustomerIDList.get(i) +"_" + lastMonthShort +".csv");
				Path outputPathAppend = new Path(LZ_PATH +"CLICK_PERFORMANCE_REPORT_" +CustomerIDList.get(i) +".csv");
				Path outputPathCurrent = new Path(LZ_PATH +"CLICK_PERFORMANCE_REPORT_" +CustomerIDList.get(i) +"_" + currentMonthShort +".csv");
				
				// Delete last month's report. 
				HDFSwriter.fileDelete(outputPathDelete);
				// Append last month's data to the historical record.
				for (LocalDate date = lastMonthDay1LD; date.isBefore(lastMonthEndLD); date = date.plusDays(1)) {
					InputStream inputStream1 = runReport(session, "CLICK_PERFORMANCE_REPORT", ReportDefinitionDateRangeType.CUSTOM_DATE, date.toString(), date.toString());
					HDFSwriter.streamAppend(inputStream1, outputPathAppend);
				}
				// Get a report for the first day of the current month and create(overwrite) it in HDFS
				InputStream inputStream3 = runReport(session, "CLICK_PERFORMANCE_REPORT", ReportDefinitionDateRangeType.CUSTOM_DATE, currentMonthDay1, currentMonthDay1);
				HDFSwriter.streamCreate(inputStream3, outputPathCurrent);
				// For days 2 through the current day of the month, get the report and append it to the current month's report.
				for (LocalDate date = currentMonthDay2LD; date.isBefore(currentMonthEndLD); date = date.plusDays(1)) {
					InputStream inputStream4 = runReport(session, "CLICK_PERFORMANCE_REPORT", ReportDefinitionDateRangeType.CUSTOM_DATE, date.toString(), date.toString());
					HDFSwriter.streamAppend(inputStream4, outputPathCurrent);
				}
			}

        }
        if (currentDayInt > 7) {
        	Credential oAuth2Credential = GetCredentials();
			for (int i=0; i < CustomerIDList.size(); i++) {
				AdWordsSession session = GetSession(CustomerIDList.get(i), oAuth2Credential);
				// Set path for the current month's report:
				Path outputPathCurrent = new Path(LZ_PATH +"CLICK_PERFORMANCE_REPORT_" +CustomerIDList.get(i) +"_" + currentMonthShort +".csv");
				// Get a report for the first day of the current month and create(overwrite) it in HDFS
				InputStream inputStream1 = runReport(session, "CLICK_PERFORMANCE_REPORT", ReportDefinitionDateRangeType.CUSTOM_DATE, currentMonthDay1, currentMonthDay1);
				HDFSwriter.streamCreate(inputStream1, outputPathCurrent);
				// For days 2 through the current day of the month, get the report and append it to the current month's report.
				for (LocalDate date = currentMonthDay2LD; date.isBefore(currentMonthEndLD); date = date.plusDays(1)) {
					InputStream inputStream2 = runReport(session, "CLICK_PERFORMANCE_REPORT", ReportDefinitionDateRangeType.CUSTOM_DATE, date.toString(), date.toString());
					HDFSwriter.streamAppend(inputStream2, outputPathCurrent);
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
			System.out.println("Unable to build AdWords session. ");
			e.printStackTrace();
		}
		return null;
	}
	public static InputStream runReport(AdWordsSession session, String ReportName, ReportDefinitionDateRangeType DateRangeType, String StartDate, String EndDate) throws Exception {
	    // Create selector. For space reasons, this ClickPerformance code doesn't use the ReportBuilder function.  All reports are selected below.		
	    Selector selector = new Selector();
		DateRange dateRange = new DateRange();
		dateRange.setMin(StartDate);
		dateRange.setMax(EndDate);
		selector.setDateRange(dateRange);
	    selector.getFields().addAll(Lists.newArrayList("AccountDescriptiveName",
                "AdFormat",
                "AdGroupId",
                "AdGroupName",
                "AdGroupStatus",
                "AdNetworkType1",
                "AdNetworkType2",
                "AoiMostSpecificTargetId",
                "CampaignId",
                "CampaignLocationTargetId",
                "CampaignName",
                "CampaignStatus",
                "ClickType",
                "CreativeId",
                "CriteriaId",
                "CriteriaParameters",
                "Date",
                "Device",
                "ExternalCustomerId",
                "GclId",
                "KeywordMatchType",
                "LopMostSpecificTargetId",
                "Page",
                "Slot",
                "UserListId"));
	    		
	    // Create report definition. DateRangeType is passed to the function.
	    ReportDefinition reportDefinition = new ReportDefinition();
	    reportDefinition.setReportName(ReportName + System.currentTimeMillis());
	    reportDefinition.setDateRangeType(DateRangeType);
	    reportDefinition.setReportType(ReportDefinitionReportType.fromValue(ReportName));
	    reportDefinition.setDownloadFormat(DownloadFormat.CSV);
	    
        
        ReportingConfiguration reportingConfiguration =
		        new ReportingConfiguration.Builder()
		            .skipReportHeader(true)
		            .skipReportSummary(true)
		            .skipColumnHeader(true)
		            // Enable to allow rows with zero impressions to show.
		            .includeZeroImpressions(false)
		            .build();
       
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
