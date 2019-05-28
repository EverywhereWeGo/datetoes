//package com.bfd;
//
//import java.io.IOException;
//
//import org.codehaus.jackson.JsonProcessingException;
//import org.codehaus.jackson.map.ObjectMapper;
//
//public class ESUtils {
//    private static ObjectMapper objectMapper = new ObjectMapper();
//    public static String toJson(Object o){
//        try {
//            return objectMapper.writeValueAsString(o);
//        } catch (JsonProcessingException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//			e.printStackTrace();
//		}
//        return "";
//    }
//}