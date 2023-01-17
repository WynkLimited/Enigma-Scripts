package com.wynk.dataplatform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class ResponseConvertor {

    private  final String basePath="http://localhost:8080/dag/dynamic/";
    private final YAMLMapper mapper=new YAMLMapper();

    private JsonNode getResponseFromEnigma(int dagId) throws IOException {

        URL url = new URL(basePath+dagId);
        return mapper.readTree(url);
    }

    public void generateDynamicDag() throws IOException {
       int dagCount=1000;

        for(int i=1;i<=dagCount;i++){
            try {
                JsonNode response= getResponseFromEnigma(i);
                System.out.println(response.toPrettyString());
                String dynamicDagFileName= response.fieldNames().next();
                String yamlString= mapper.writeValueAsString(response);
           //     System.out.println(yamlString);
                updateAndSaveDynamicDagYamlFile(dynamicDagFileName,yamlString);
            }
            catch (IOException e){
                System.out.println("Scheduled "+ (i-1) +" dynamic dags from Enigma ");
                return;
            }
        }
    }

    private void updateAndSaveDynamicDagYamlFile(String dynamicDagFileName,String yamlString) throws IOException {
        String updatedYamlString = yamlString.replaceAll("- [a-zA-z]"," d");

        FileOutputStream writer= new FileOutputStream("./GeneratedDynamicYamlDags/"+dynamicDagFileName+".yaml");
        writer.write(updatedYamlString.getBytes());
        writer.close();

        System.out.println(dynamicDagFileName + " saved successfully");
    }
}
