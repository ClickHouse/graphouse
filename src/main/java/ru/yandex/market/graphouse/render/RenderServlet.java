package ru.yandex.market.graphouse.render;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 15/02/2017
 */
public class RenderServlet extends HttpServlet {

    private final RenderService renderService;

    public RenderServlet(RenderService renderService) {
        this.renderService = renderService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
//        super.doGet(req, resp);

        String[] targets = req.getParameterValues("target");
        String maxDataPointsString = req.getParameter("maxDataPoints");
        int maxDataPoints = (maxDataPointsString == null) ? -1 : Integer.valueOf(maxDataPointsString);

        String target = req.getParameter("target");

        try {
            renderService.doWork(
                1487106000,
                1487192459,
                target,
//                "aliasByNode(averageAbove(one_min.market-front.errors-dynamic.5xx-percent.*.*, 0.1), 3, 5)",
//                "aliasByNode(averageAbove(one_min.market-front.errors-dynamic.5xx-percent.page.api_data-collector, 0.1), 3, 5)",
                424,
                resp.getWriter()
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        super.doPost(req, resp);
    }


}
