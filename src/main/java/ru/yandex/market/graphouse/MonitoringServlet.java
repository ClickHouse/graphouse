package ru.yandex.market.graphouse;

import ru.yandex.market.graphouse.monitoring.Monitoring;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 27/04/15
 */
public class MonitoringServlet extends HttpServlet {

    private final Monitoring monitoring;
    private final Monitoring ping;

    public MonitoringServlet(Monitoring monitoring, Monitoring ping) {
        this.monitoring = monitoring;
        this.ping = ping;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        switch (req.getRequestURI()) {
            case "/ping":
                writeResponse(resp, ping);
                break;
            case "/monitoring":
                writeResponse(resp, monitoring);
                break;
            default:
                resp.getWriter().print("Bad request");
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                break;
        }
    }

    private static void writeResponse(HttpServletResponse resp, Monitoring monitoring) throws IOException {

        Monitoring.Result result = monitoring.getResult();
        switch (result.getStatus()) {
            case OK:
            case WARNING:
                resp.setStatus(HttpServletResponse.SC_OK);
                break;
            case CRITICAL:
                resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                break;
            default:
                throw new IllegalStateException();
        }
        resp.getWriter().print(result.toString());
    }


}
