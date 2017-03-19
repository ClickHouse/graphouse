package ru.yandex.market.graphouse;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ru.yandex.market.graphouse.config.GraphouseConfig;

/**
 * @author Vlad Vinogradov <a href="mailto:vladvin@yandex-team.ru"></a>
 * @date 10.11.16
 */
public class GraphouseMain {
    private static final Logger log = LogManager.getLogger();

    private GraphouseMain() {
    }

    public static void main(String[] args) throws Exception {

        System.out.println("Graphouse path is: " + System.getProperty("app.home"));

        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();

        applicationContext.registerShutdownHook();
        applicationContext.register(GraphouseConfig.class);
        applicationContext.refresh();

        log.info("Graphouse up and running");

        Thread.setDefaultUncaughtExceptionHandler((t, e) -> log.error("Uncaught Exception in thread " + t.toString(), e));
    }
}
