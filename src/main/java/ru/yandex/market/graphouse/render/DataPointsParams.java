package ru.yandex.market.graphouse.render;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 * @date 15/05/2017
 */
public class DataPointsParams {
    private final int startTimeSeconds;
    private final int stepSeconds;
    private final int pointsCount;

    public DataPointsParams(int startTimeSeconds, int stepSeconds, int pointsCount) {
        this.startTimeSeconds = startTimeSeconds;
        this.stepSeconds = stepSeconds;
        this.pointsCount = pointsCount;
    }

    public int getStartTimeSeconds() {
        return startTimeSeconds;
    }

    public int getStepSeconds() {
        return stepSeconds;
    }

    public int getPointsCount() {
        return pointsCount;
    }

    public int getEndTimeSeconds() {
        return startTimeSeconds + stepSeconds + pointsCount; //TODO check end
    }
}
