package ru.yandex.market.graphouse;

import org.springframework.beans.factory.annotation.Required;
import ru.yandex.market.monitoring.ComplicatedMonitoring;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 27/04/15
 */
public class MonitoringServlet extends HttpServlet {
    private ComplicatedMonitoring monitoring;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ComplicatedMonitoring.Result result = monitoring.getResult();
        switch (result.getStatus()) {
            case OK:
            case WARNING:
                resp.setStatus(HttpServletResponse.SC_OK);
                break;
            case CRITICAL:
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                break;
            default:
                throw new IllegalStateException();
        }
        resp.getWriter().print(result.toString());
    }

    @Required
    public void setMonitoring(ComplicatedMonitoring monitoring) {
        this.monitoring = monitoring;
    }
}
