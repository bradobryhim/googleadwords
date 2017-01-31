package com.graybar.dd.adwords.prod;

import java.io.InputStream;
import java.util.ArrayList;
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
import com.google.api.client.auth.oauth2.Credential;
import com.google.common.collect.Lists;

public class HistoricalReporting {
	
	public static final String DATE_MIN = null;
	public static final String DATE_MAX = null;
	public static final String LZ_PATH = "hdfs://cplhdpmp01.graybar.com/data_LZ/Digital_Business/API_Integration/Google_AdWords/";
	
	public static void main(String[] args) throws Exception {
		Credential oAuth2Credential = new OfflineCredentials.Builder()
			.forApi(Api.ADWORDS)
	        .fromFile()
	        .build()
	        .generateCredential();

        ArrayList<String> CustomerIDList = new ArrayList<String>(); 
        CustomerIDList.addAll(Lists.newArrayList(
        		"3107410061",
        		"6071514471",
        		"8031908053",
        		"8445299289",
        		"9417573739",
        		"9521584010"
        		));
        
        ArrayList<String> ReportList = new ArrayList<String>();
        ReportList.addAll(Lists.newArrayList(
//        		"ACCOUNT_PERFORMANCE_REPORT",
//        		"ADGROUP_PERFORMANCE_REPORT",
//                "AD_PERFORMANCE_REPORT",
//                "AUDIENCE_PERFORMANCE_REPORT",
//                "AUTOMATIC_PLACEMENTS_PERFORMANCE_REPORT",
//                "CAMPAIGN_LOCATION_TARGET_REPORT",
//                "CAMPAIGN_PERFORMANCE_REPORT",
////                "CLICK_PERFORMANCE_REPORT",
//                "FINAL_URL_REPORT",
//                "GEO_PERFORMANCE_REPORT",
//                "KEYWORDLESS_CATEGORY_REPORT",
                "KEYWORDLESS_QUERY_REPORT"
//                "KEYWORDS_PERFORMANCE_REPORT",
//                "PAID_ORGANIC_QUERY_REPORT",
//                "PLACEMENT_PERFORMANCE_REPORT",
//                "PRODUCT_PARTITION_REPORT",
//                "SEARCH_QUERY_PERFORMANCE_REPORT",
//                "SHARED_SET_REPORT",
//                "SHARED_SET_CRITERIA_REPORT"
//                "SHOPPING_PERFORMANCE_REPORT",
//                "URL_PERFORMANCE_REPORT",
//                "USER_AD_DISTANCE_REPORT"
                ));
        
		// Construct an AdWordsSession.
        for (int i = 0; i < CustomerIDList.size(); i++) {
        	AdWordsSession session = new AdWordsSession.Builder()
        			.fromFile()
        			.withClientCustomerId(CustomerIDList.get(i))
        			.withOAuth2Credential(oAuth2Credential)
        			.build();
        	
        	for (int j = 0; j < ReportList.size(); j++) {
        		Path path = new Path(LZ_PATH + ReportList.get(j) + "/" + ReportList.get(j) + "_" + CustomerIDList.get(i) +".csv");
        		
        		InputStream InputStream = null;
				try {
					InputStream = (runReport(session, ReportList.get(j)));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}        		     		
                HDFSwriter.streamCreate(InputStream, path);
        	}
        }
	}

	  public static InputStream runReport(AdWordsSession session, String ReportName) throws Exception {
		    // Create selector.
		    Selector selector = ReportBuilder.ReportBuilder(ReportName, DATE_MIN, DATE_MAX);

		    // Create report definition.
		    ReportDefinition reportDefinition = new ReportDefinition();
		    reportDefinition.setReportName(ReportName + System.currentTimeMillis());
		    reportDefinition.setDateRangeType(ReportDefinitionDateRangeType.ALL_TIME);
		    reportDefinition.setReportType(ReportDefinitionReportType.fromValue(ReportName));
		    reportDefinition.setDownloadFormat(DownloadFormat.CSV);
		    
		    // Optional: Set the reporting configuration of the session to suppress header, column name, or
		    // summary rows in the report output. You can also configure this via your ads.properties
		    // configuration file. See AdWordsSession.Builder.from(Configuration) for details.
		    // In addition, you can set whether you want to explicitly include or exclude zero impression
		    // rows.
		    
		    ArrayList<String> zeroImpressionSupport = new ArrayList<String>(); 
	        zeroImpressionSupport.addAll(Lists.newArrayList(
	        		"ACCOUNT_PERFORMANCE_REPORT",
//	        		"ADGROUP_PERFORMANCE_REPORT",
	        		"AUDIENCE_PERFORMANCE_REPORT",
	        		"CAMPAIGN_LOCATION_TARGET_REPORT",
	        		"CAMPAIGN_PERFORMANCE_REPORT",
//	        		"KEYWORDS_PERFORMANCE_REPORT",
	        		"PLACEMENT_PERFORMANCE_REPORT",
	        		"PRODUCT_PARTITION_REPORT"));
	        
	        ReportingConfiguration reportingConfiguration = null;
	        
	        if (zeroImpressionSupport.contains(ReportName)) {
			    reportingConfiguration =
				        new ReportingConfiguration.Builder()
				            .skipReportHeader(true)
				            .skipColumnHeader(false)
				            .skipReportSummary(true)
				            // Enable to allow rows with zero impressions to show.
				            .includeZeroImpressions(true)
				            .build();
	        }
	        else {
			    reportingConfiguration =
				        new ReportingConfiguration.Builder()
				            .skipReportHeader(true)
				            .skipColumnHeader(false)
				            .skipReportSummary(true)
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
