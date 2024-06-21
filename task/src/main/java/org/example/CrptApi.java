package org.example;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import jakarta.validation.constraints.NotNull;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Сервис отправки запросов с ограничением по количеству отправок на единицу времени
 *
 * @author Kuranov
 * @date 21-06-2024
 */
public class CrptApi {


    // URL для запросов
    private static final String REQUEST_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";

    // Форматтер для логирования
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH-mm-ss");

    // Очередь записей, содержащих JSON строку запроса и callback
    private final LinkedBlockingDeque<RequestRecord> requestRecords = new LinkedBlockingDeque<>();

    // JSON writer
    private final ObjectWriter writer = new ObjectMapper().writer().withDefaultPrettyPrinter();

    // HTTP клиент
    private final HttpClient client = HttpClient.newBuilder().build();

    // Лимит запросов в единицу времени
    private final Integer requestLimit;

    // Единица времени ограничения запросов
    private final TimeUnit timeUnit;

    // Сервис исполнения потоков
    private final ScheduledExecutorService executorService;

    // Итератор номера для логов
    private final AtomicInteger num = new AtomicInteger(1);


    /**
     * Создаёт экземпляр сервиса отправки запросов с ограничением количества запросов в единицу времени
     *
     * @param requestLimit количество запросов
     * @param timeUnit     единица времени
     */
    public CrptApi(@NotNull Integer requestLimit, @NotNull TimeUnit timeUnit) {
        this.requestLimit = requestLimit;
        this.timeUnit = timeUnit;
        this.executorService = Executors.newScheduledThreadPool(requestLimit);

        performRequests();
    }

    /**
     * Отправляет запросы из очереди, ограничивая их по количеству за единицу времени,
     * получает ответ, вызывает callback
     */
    private void performRequests() {
        log(0, " start service ...");
        executorService.scheduleAtFixedRate(() -> {
            for (int i = 0; i < requestLimit; i++) {
                RequestRecord requestRecord = requestRecords.poll();
                if (requestRecord != null) {
                    new Thread(() -> {
                        int number = num.getAndIncrement();

                        log(number, " create request ...");
                        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(REQUEST_URL)).POST(HttpRequest.BodyPublishers.ofString(requestRecord.requestBody)).build();

                        try {
                            log(number, " send request ...");
                            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                            log(number, " receive response ...");
                            Consumer<HttpResponse<String>> onResponse = requestRecord.onResponse;

                            if (onResponse != null) {
                                log(number, " invoke callback with response ...");
                                onResponse.accept(response);
                            }
                        } catch (IOException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }).start();
                }
            }
        }, 0L, 1, timeUnit);
    }

    /**
     * Сериализует запрос в JSON, упаковывает его вместе с callback в record и добавляет его в очередь
     *
     * @param requestBodyDTO DTO запроса
     * @param onResponse     callback, вызывается по возвращении ответа
     */
    public void addRequest(RequestBodyDTO requestBodyDTO, Consumer<HttpResponse<String>> onResponse) {
        String requestBody;
        try {
            requestBody = writer.writeValueAsString(requestBodyDTO);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        RequestRecord requestRecord = new RequestRecord(requestBody, onResponse);
        requestRecords.add(requestRecord);
    }

    /**
     * Метод логирования
     *
     * @param number  порядковый номер запроса
     * @param message сообщение
     */
    private void log(int number, String message) {
        if (number == 0) {
            System.out.println(LocalDateTime.now().format(dateTimeFormatter) + " " + message);
        } else {
            System.out.println(LocalDateTime.now().format(dateTimeFormatter) + " " + number + message);
        }
    }

    /**
     * Завершает работу сервиса отправки запросов, возвращает не отправленные запросы
     *
     * @return список не отправленных запросов
     */
    public List<RequestBodyDTO> shutdownService() {
        log(0, " stop service ...");
        executorService.shutdownNow();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return requestRecords.stream().map(requestRecord -> {
            try {
                return mapper.readValue(requestRecord.requestBody, RequestBodyDTO.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).toList();

    }

    /**
     * Контенейнер для запроса и callback
     *
     * @param requestBody запрос в виде JSON строки
     * @param onResponse  callback, вызывается по возвращении ответа
     */
    record RequestRecord(String requestBody, Consumer<HttpResponse<String>> onResponse) {
    }

    /**
     * Контейнер запроса
     */
    public static class RequestBodyDTO {
        public Description description = new Description();
        public String doc_id = "string";
        public String doc_status = "string";
        public DocType doc_type = DocType.LP_INTRODUCE_GOODS;
        public boolean importRequest = Boolean.TRUE;
        public String owner_inn = "string";
        public String participant_inn = "string";
        public String producer_inn = "string";

        @JsonFormat(pattern = "yyyy-MM-dd")
        @JsonSerialize(using = LocalDateSerializer.class)
        public LocalDate production_date = LocalDate.of(2020, 1, 23);

        public String production_type = "string";
        public List<Product> products = List.of(new Product());

        @JsonFormat(pattern = "yyyy-MM-dd")
        @JsonSerialize(using = LocalDateSerializer.class)
        public LocalDate reg_date = LocalDate.of(2020, 1, 23);

        public String reg_number = "string";
    }

    /**
     * Контейнер продукта
     */
    public static class Product {
        public String certificate_document = "string";

        @JsonFormat(pattern = "yyyy-MM-dd")
        @JsonSerialize(using = LocalDateSerializer.class)
        public LocalDate certificate_document_date = LocalDate.of(2020, 1, 23);

        public String certificate_document_number = "string";
        public String owner_inn = "string";
        public String producer_inn = "string";

        @JsonFormat(pattern = "yyyy-MM-dd")
        @JsonSerialize(using = LocalDateSerializer.class)
        public LocalDate production_date = LocalDate.of(2020, 1, 23);

        public String tnved_code = "string";
        public String uit_code = "string";
        public String uitu_code = "string";
    }

    /**
     * Типы документов
     */
    public enum DocType {
        LP_INTRODUCE_GOODS;
    }

    /**
     * Контейнер описания
     */
    public static class Description {
        public String participantInn = "string";
    }
}
