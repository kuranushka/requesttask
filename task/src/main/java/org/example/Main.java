package org.example;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {

        CrptApi crptApi = new CrptApi(50, TimeUnit.SECONDS);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < 1000; i++) {
            crptApi.addRequest(new CrptApi.RequestBodyDTO(), System.out::println);
        }

        try {
            Thread.sleep(2000);
            List<CrptApi.RequestBodyDTO> requestBodyDTOS = crptApi.shutdownService();
            System.out.printf("queued: %d requests\n", requestBodyDTOS.size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}