package com.baidu.spark.history;

import com.baidu.spark.util.ConfUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;

public class FindRetryStages {
    private static final String SPARK_HISTORY_SERVER = "spark.history.server.address";
    private static final String SPARK_APP_CHECK_MIN_DURATION_MS = "spark.app.check.min.duration.ms";
    private static final long SPARK_APP_CHECK_MIN_DURATION_MS_DEFAULT = 10 * 60 * 1000L;
    private static ObjectMapper mapper = new ObjectMapper();

    private final String applicationsUrl;
    private final String minEndDate;
    private final String maxEndDate;
    private final long minDuration;

    public static void main(String[] args) throws IOException {
        Configuration conf = ConfUtils.getConf(args);
        String historyServer = conf.get(SPARK_HISTORY_SERVER);
        long minDuration = conf.getLong(SPARK_APP_CHECK_MIN_DURATION_MS, SPARK_APP_CHECK_MIN_DURATION_MS_DEFAULT);
        if (historyServer == null) {
            System.out.println("Please set " + SPARK_HISTORY_SERVER);
            return;
        }
        if (historyServer.endsWith("/")) {
            historyServer = historyServer.substring(0, historyServer.length() - 1);
        }
        String applicationsUrl = historyServer + "/api/v1/applications";
        String minEndDate = conf.get("minEndDate");
        String maxEndDate = conf.get("maxEndDate");
        if (minEndDate == null || maxEndDate == null) {
            System.out.println("Please set minEndDate and maxEndDate");
            return;
        }

        FindRetryStages findRetryStages = new FindRetryStages(applicationsUrl,
                minEndDate, maxEndDate, minDuration);
        findRetryStages.process();
    }

    public FindRetryStages(String applicationsUrl, String minEndDate, String maxEndDate, long minDuration) {
        this.applicationsUrl = applicationsUrl;
        this.minEndDate = minEndDate;
        this.maxEndDate = maxEndDate;
        this.minDuration = minDuration;
    }

    public void process() throws IOException {
        String url = applicationsUrl + "?minEndDate=" + minEndDate + "&maxEndDate=" + maxEndDate;
        String response = executeHttpRequest(url);
        if (response == null) {
            System.out.println("applications Response is null");
            return;
        }
        processApplications(response);
    }

    private void processApplications(String response) throws IOException {
        JsonNode appsNode = mapper.readTree(response);
        if (! appsNode.isArray()) {
            System.out.println("appsNode is not array ");
            return;
        }
        Iterator<JsonNode> appIt = appsNode.iterator();
        while (appIt.hasNext()) {
            JsonNode app = appIt.next();
            processApplication(app);
        }
    }

    private void processApplication(JsonNode app) throws IOException {
        String id = app.get("id").asText();
        boolean isLocal = id.startsWith("local");
        System.out.println("find application " + id);
        JsonNode attemptsNode = app.get("attempts");
        if (! attemptsNode.isArray()) {
            System.err.println("attemptsNode is not array " + attemptsNode);
            return;
        }
        int size = attemptsNode.size();
        System.out.println("application " + id + "attempt size:" + size);
        for (int i = 0; i < size; i++) {
            JsonNode attemptNode = attemptsNode.get(i);
            long duration = attemptNode.get("duration").asLong();
            if (duration < minDuration) {
                continue;
            }
            String url = applicationsUrl + "/" + id +  "/" + (isLocal? "" : (i + 1) + "/" ) + "stages";
            String stagesResponse = executeHttpRequest(url);
            if (stagesResponse == null) {
                System.out.println("stagesResponse is null");
                continue;
            }
            // System.out.println("stagesResponse" + stagesResponse);
            JsonNode stagesNode = mapper.readTree(stagesResponse);
            Iterator<JsonNode> stages = stagesNode.iterator();
            while (stages.hasNext()) {
                JsonNode stage = stages.next();
                int  attemptId = stage.get("attemptId").asInt();
                if (attemptId != 0) {
                    System.out.println("ERROR: " + id + " stages has retries" );
                } else {
                    System.out.println("INFO: " + id + " stages has no retries" );
                }
            }
        }
    }

    public static String executeHttpRequest(String targetURL) {
        System.out.println("executeHttpRequest for " + targetURL);
        HttpURLConnection connection = null;

        try {
            //Create connection
            URL url = new URL(targetURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("accept", "application/json");

            connection.setUseCaches(false);
            connection.setDoOutput(true);
            int responseCode = connection.getResponseCode();
            if (responseCode != 200) {
                System.out.println("Response code is " + responseCode + " for url:" + targetURL);
                return null;
            }

            //Get Response
            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            StringBuilder response = new StringBuilder(); // or StringBuffer if Java version 5+
            String line;
            while ((line = rd.readLine()) != null) {
                response.append(line);
                response.append('\r');
            }
            rd.close();
            return response.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
