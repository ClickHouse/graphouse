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

    public static void main(String[] args) {

        System.out.println("Graphouse path is: " + System.getProperty("app.home"));

        System.setProperty("environment", System.getProperty("environment", "default"));
        System.out.println("Environment: " + System.getProperty("environment"));

        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();

        applicationContext.registerShutdownHook();
        Thread.setDefaultUncaughtExceptionHandler(
            (t, e) -> log.error("Uncaught Exception in thread " + t.toString(), e)
        );
        applicationContext.register(GraphouseConfig.class);


        try {
            applicationContext.refresh();
            log.info("Graphouse up and running");
        } catch (Exception e) {
            log.error("Graphouse failed to start", e);
            System.exit(1);
        }

    }
}
