package com.wynk.dataplatform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;

import javax.swing.text.html.Option;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MigrationScript {

    private final ObjectMapper mapper=new ObjectMapper();
    private final String metaPath="http://dp.wynk.in/dataset/";
    private final String enigmaPath="http://localhost:8080/";
    private final String jsonBaseFilePath="/Users/b0273254/Desktop/DP/DP-Updated/data-platform/jobs/partner/base/";
    private final String ingestionJarPath="s3://data-platform-test-only/shivank/data-platform-ingestion-enigma-Xstream-billing-assembly-1.00.jar";
    private final String dagName="dp-partner-prod-data-daily-end-workflow";


   public JsonNode addSparkSubmitTask(JsonNode jsonConfigFile,String datasetName) throws IOException {
       List<String> sparkConfig= new ArrayList<>();
       List<String> ingestionConfig= new ArrayList<>();
       List<String> dependencies = new ArrayList<>();
       HashMap<String,String> defaultProps = new HashMap<>();

       if(jsonConfigFile.get("sparkConfig")!=null){
           jsonConfigFile.get("sparkConfig").forEach(prop ->{
               if(prop.asText().startsWith("--conf")){
                   sparkConfig.add(prop.asText().replaceFirst("^--conf ",""));
               }else {
                   defaultProps.put(prop.asText().replaceFirst("^--","").split(" ")[0],
                                    prop.asText().replaceFirst("^--","").split(" ")[1]);
               }
           });
       }

       if(jsonConfigFile.get("partnerId")!=null){
           ingestionConfig.add("partner.runid="+jsonConfigFile.get("partnerId").asText());
       }

       if(jsonConfigFile.get("ingestionConfig")!=null){
           jsonConfigFile.get("ingestionConfig").forEach(conf->{
               ingestionConfig.add(conf.asText().replaceFirst("^--ingestion.conf ",""));
           });
       }
       if (jsonConfigFile.get("dependentTableWithName")!=null){
           jsonConfigFile.get("dependentTableWithName").forEach(dependency ->{
               dependencies.add(dependency.get("jobName").asText());
           });
       }
//       System.out.println(sparkConfig);
//       System.out.println(defaultProps);
//       System.out.println(ingestionConfig);
//       System.out.println(dependencies);

       ObjectNode task = mapper.createObjectNode();

       task.put("taskName",jsonConfigFile.get("name").asText())
                   .put("operator","sparkSubmit")
                   .put("createdBy","shivank.chaturvedi@wynk.in");

       defaultProps.keySet().forEach(key -> {
           task.with("sparkSubmitConfigs").put(key,defaultProps.get(key));
       });

       task.with("sparkSubmitConfigs")
               .put("datasetName",datasetName)
               .put("jarPath",ingestionJarPath)
               .putArray("sparkConfig").addAll((ArrayNode) mapper.valueToTree(sparkConfig));

       task.putArray("ingestionConfig").addAll((ArrayNode) mapper.valueToTree(ingestionConfig));
       task.putArray("dependencies").addAll((ArrayNode) mapper.valueToTree(dependencies));

       System.out.println(task.toPrettyString());

       CloseableHttpClient httpClient = HttpClientBuilder.create().build();
       try {
           URI uri = new URIBuilder(enigmaPath+"task")
                   .build();
           HttpPost request = new HttpPost(uri);

           StringEntity params = new StringEntity(task.toString());
           request.addHeader("content-type", "application/json");
           request.setEntity(params);
//            System.out.println(request);
           HttpEntity entity=httpClient.execute(request).getEntity();
           JsonNode apiResponse=mapper.readTree(EntityUtils.toString(entity, StandardCharsets.UTF_8));
           return apiResponse;
       } catch (Exception ex) {
           ex.printStackTrace();
       } finally {
           httpClient.close();
       }
       return  null;
   }
   public JsonNode getJsonFileNode(String jobName) throws IOException {
       File jobFile= new File(jsonBaseFilePath+jobName+".json") ;
        return  mapper.readTree(jobFile);
   }

    public JsonNode addDataset(JsonNode datasetFull) throws IOException {
        ObjectNode enigmaDataset = mapper.createObjectNode();

        enigmaDataset.put("datasetName",datasetFull.get("name").asText());
        enigmaDataset.put("tableName",datasetFull.get("name").asText().split("\\.")[1]+"_enigma");
        enigmaDataset.put("createdBy","shivank.chaturvedi@wynk.in");
        enigmaDataset.put("datsetDescription","dataset migrated via script");
        enigmaDataset.put("legacyDatasetType","base");

 //       System.out.println(enigmaDataset.toPrettyString());
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        try {
            URI uri = new URIBuilder(enigmaPath+"dataset")
                    .addParameter("domainId", "1")
                    .build();
            HttpPost request = new HttpPost(uri);

            StringEntity params = new StringEntity(enigmaDataset.toString());
            request.addHeader("content-type", "application/json");
            request.setEntity(params);
//            System.out.println(request);
            HttpEntity entity=httpClient.execute(request).getEntity();
            JsonNode apiResponse=mapper.readTree(EntityUtils.toString(entity, StandardCharsets.UTF_8));
            return apiResponse;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            httpClient.close();
        }

        return null;
    }

    public  void addSource(JsonNode source){
        if(source==null){
            return;
        }
    }
    public Integer addSink(JsonNode sink) throws IOException {
         String platformType = null;
        ObjectNode enigmaSink = mapper.createObjectNode();
        enigmaSink.put("sinkName",sink.get("name").asText()+"_sink_enigma_"+sink.get("id"));
        enigmaSink.putArray("partitionFields")
                .addAll((ArrayNode) mapper.valueToTree(sink.get("partitionField").asText().split(",")));
        enigmaSink.put("sinkDescription","script to migrate sinks in enigma");
        enigmaSink.put("schema","partner");

        if(sink.get("destinationType").asText().equalsIgnoreCase("s3")){
            platformType="s3";
            enigmaSink.with("s3Params").put("s3Path",sink.get("location").asText());
        }
        if (sink.get("destinationType").asText().equalsIgnoreCase("kafka")){
            platformType="kafka";
            enigmaSink.with("kafkaParams")
                    .put("extraOptions",sink.get("metaConfig").get("extraOptions").asText())
                    .put("bootstrapServers",sink.get("metaConfig").get("bootstrapServers").asText())
                    .put("kafkaTopic",sink.get("metaConfig").get("subscribeTopic").asText())
                    .put("key",sink.get("metaConfig").get("key").asText());

        }

        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        try {
            URI uri = new URIBuilder(enigmaPath+"sink")
                    .addParameter("platformType", platformType)
                    .build();
            HttpPost request = new HttpPost(uri);

            StringEntity params = new StringEntity(enigmaSink.toString());
            request.addHeader("content-type", "application/json");
            request.setEntity(params);
//            System.out.println(request);
            HttpEntity entity=httpClient.execute(request).getEntity();
            JsonNode apiResponse=mapper.readTree(EntityUtils.toString(entity, StandardCharsets.UTF_8));
            return apiResponse.get("sinkId").asInt();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            httpClient.close();
        }

        return -1;

    }
    public JsonNode getMetaResponse(Integer datasetId) throws IOException {
            URL url = new URL(metaPath + datasetId+ "/full");
           return mapper.readTree(url);
    }
    public void addSinkToPipeline(Integer pipelineStepId,Integer sinkId) throws  Exception{
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        try {
           URI uri = new URIBuilder(enigmaPath+"pipeline/addSink/"+pipelineStepId)
                   .addParameter("sinkId", sinkId.toString())
                   .build();
           HttpPut request = new HttpPut(uri);
           request.addHeader("content-type", "application/json");
           httpClient.execute(request);
       }
        catch (Exception ex) {
            ex.printStackTrace();
           }
    }
    public void scheduleTask(JsonNode task){
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();

        Integer taskId= task.get("taskId").asInt();
        try {
            URI uri = new URIBuilder(enigmaPath+"task/schedule/"+taskId)
                    .addParameter("dagName", dagName)
                    .build();
            HttpPut request = new HttpPut(uri);
            request.addHeader("content-type", "application/json");
            httpClient.execute(request);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public void migrateEtlJob() throws IOException,Exception {
        HashMap<Integer,List<String> >datasetMapping = new HashMap<Integer,List<String>>(){{
            put(501,Arrays.asList("dp-partner-prod-music-cms-daily-report"));
            put(548,Arrays.asList("dp-partner-prod-mtd-user-type-days"));
            put(504,Arrays.asList("dp-partner-prod-music-cms-mtd-report"));
            put(505,Arrays.asList("dp-partner-prod-music-cms-daily-report-csv"));
            put(414,Arrays.asList("dp-partner-prod-music-billing-wmg",
                    "dp-partner-prod-music-billing-abcdigital",
                    "dp-partner-prod-music-billing-adityamusic",
                    "dp-partner-prod-music-billing-believe",
                    "dp-partner-prod-music-billing-divo",
                    "dp-partner-prod-music-billing-eveara",
                    "dp-partner-prod-music-billing-gkdigital",
                    "dp-partner-prod-music-billing-horus",
                    "dp-partner-prod-music-billing-hungama",
                    "dp-partner-prod-music-billing-ingrooves",
                    "dp-partner-prod-music-billing-monstercat",
                    "dp-partner-prod-music-billing-nuemeta",
                    "dp-partner-prod-music-billing-onedigital",
                    "dp-partner-prod-music-billing-orchard",
                    "dp-partner-prod-music-billing-pdl",
                    "dp-partner-prod-music-billing-saregama",
                    "dp-partner-prod-music-billing-simca",
                    "dp-partner-prod-music-billing-sony",
                    "dp-partner-prod-music-billing-speedmusic",
                    "dp-partner-prod-music-billing-timesmusic",
                    "dp-partner-prod-music-billing-unisysinfo",
                    "dp-partner-prod-music-billing-universalmusic",
                    "dp-partner-prod-music-billing-venus",
                    "dp-partner-prod-music-billing-zeemusic"));
            put(506,Arrays.asList("dp-partner-prod-music-cms-mtd-report-csv"));
            put(533,Arrays.asList("dp-partner-prod-music-billing-ytd-wmg"));
        }};
       for(Integer x: datasetMapping.keySet()){
           JsonNode datasetFull= getMetaResponse(x);
            JsonNode datasetResponse=addDataset(datasetFull.get("dataset"));
           System.out.println(datasetResponse);
           datasetFull.get("destinations").forEach(sink-> {
               try {
                   Integer sinkId=addSink(sink);
                   addSinkToPipeline(datasetResponse.get("datasetId").asInt(),sinkId);

               } catch (Exception e) {
                   throw new RuntimeException(e);
               }
           });
            addSource(datasetFull.get("source"));
           datasetMapping.get(x).forEach(jobName ->{
               try {
                 JsonNode task= addSparkSubmitTask(getJsonFileNode(jobName),datasetResponse.get("datasetName").asText());
                 scheduleTask(task);
               } catch (IOException e) {
                   throw new RuntimeException(e);
               }
           });
       }
    }

    public void migrateCheckJob(){
       List<String> checkJobs= Arrays.asList("dp-partner-prod-music-billing-nuemeta",
                                             "dp-partner-prod-music-cms-mtd-report-csv");
       checkJobs.forEach(jobName->{
           try {
                addSparkSubmitTask(getJsonFileNode(jobName),"partner.mtd_user_type_days");
           } catch (IOException e) {
               throw new RuntimeException(e);
           }
       });

    }

}
