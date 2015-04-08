package ru.yandex.market.graphouse.search;

import ru.yandex.market.graphouse.Metric;

import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"/>
 * @date 07/04/15
 */
public class MetricTree {

    private final Dir root = new Dir("");

    //TODO передавать сюда стрим
    public String search(String query) {
        StringBuilder answer = new StringBuilder();
        String[] levels = query.split("\\.");
        search(root, levels, 0, answer);
        if (answer.length() > 0) {
            answer.deleteCharAt(answer.length() - 1);
        }
        return answer.toString();
    }


    private void search(Dir parentDir, String[] levels, int levelIndex, StringBuilder answer) {
        if (parentDir.ban) {
            return;
        }
        boolean isLast = (levelIndex == levels.length - 1);
        String level = levels[levelIndex];
        boolean pattern = containsExpressions(level);

        if (!pattern) {
            if (isLast) {
                addSimpleAnswer(parentDir, level, answer);
            } else {
                Dir dir = parentDir.dirs.get(level);
                if (dir != null) {
                    search(dir, levels, levelIndex + 1, answer);
                }
            }
        } else if (level.equals("*")) {
            if (isLast) {
                addAllAnswer(parentDir, answer);
            } else {
                for (Dir dir : parentDir.dirs.values()) {
                    search(dir, levels, levelIndex + 1, answer);
                }
            }
        } else {
            //TODO нахрен переписать
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + level);
            if (isLast) {
                addPatternAnswer(parentDir, matcher, answer);
            } else {
                for (Map.Entry<String, Dir> dirEntry : parentDir.dirs.entrySet()) {
                    Path path = Paths.get(dirEntry.getKey());
                    if (matcher.matches(path)) {
                        search(dirEntry.getValue(), levels, levelIndex + 1, answer);
                    }
                }
            }
        }
    }

    private void addSimpleAnswer(Dir parentDir, String name, StringBuilder answer) {
        addAnswer(parentDir.dirs.get(name), answer);
        addAnswer(parentDir.metrics.get(name), answer);
    }

    private void addAllAnswer(Dir parentDir, StringBuilder answer) {
        for (Dir dir : parentDir.dirs.values()) {
            addAnswer(dir, answer);
        }
        for (MetricName metric : parentDir.metrics.values()) {
            addAnswer(metric, answer);
        }
    }

    private void addPatternAnswer(Dir parentDir, PathMatcher matcher, StringBuilder answer) {
        for (Map.Entry<String, Dir> dirEntry : parentDir.dirs.entrySet()) {
            Path path = Paths.get(dirEntry.getKey());
            if (matcher.matches(path)) {
                addAnswer(dirEntry.getValue(), answer);
            }
        }
        for (Map.Entry<String, MetricName> metricEntry : parentDir.metrics.entrySet()) {
            Path path = Paths.get(metricEntry.getKey());
            if (matcher.matches(path)) {
                addAnswer(metricEntry.getValue(), answer);
            }
        }
    }

    private void addAnswer(Dir dir, StringBuilder answer) {
        if (dir != null && !dir.ban) {
            answer.append(dir.fullName).append('\n');
        }
    }

    private void addAnswer(MetricName metric, StringBuilder answer) {
        if (metric != null && !metric.ban) {
            answer.append(metric.fullName).append('\n');
        }
    }

    /**
     * @param metric
     * @return true - если новая метрика была добавлена
     */
    public boolean add(String metric) {
        return modify(metric, false);
    }


    public void ban(String metric) {
        modify(metric, true);
    }

    private boolean modify(String metric, boolean ban) {
        if (containsExpressions(metric)) {
            return false;
        }
        boolean isDir = metric.charAt(metric.length() - 1) == '.';
        if (isDir && !ban) {
            return false;
        }

        String[] levels = metric.split("\\.");
        Dir dir = root;
        for (int i = 0; i < levels.length; i++) {
            if (dir.ban) {
                return false;
            }
            String level = levels[i];
            boolean isLast = (i == levels.length - 1);
            if (!isLast) {
                dir = dir.getOrCreateDir(level);
            } else {
                if (ban) {
                    ban(dir, level, isDir);
                    return true;
                } else {
                    return dir.createMetric(level);
                }
            }
        }
        throw new IllegalStateException();
    }

    private void ban(Dir parent, String name, boolean isDir) {
        if (isDir) {
            parent.getOrCreateDir(name).ban = true;
        } else {
            parent.getOrCreateMetric(name).ban = true;
        }
    }

    private boolean containsExpressions(String metric) {
        return metric.contains("*") || metric.contains("?");
    }

    private static class Dir {
        private final String fullName;
        private final ConcurrentMap<String, MetricName> metrics = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Dir> dirs = new ConcurrentHashMap<>();
        private volatile boolean ban = false;

        public Dir(String fullName) {
            this.fullName = fullName;
        }

        public Dir(String fullName, boolean ban) {
            this.fullName = fullName;
            this.ban = ban;
        }

        private Dir getOrCreateDir(String name) {
            Dir dir = dirs.get(name);
            if (dir != null) {
                return dir;
            }
            Dir newDir = new Dir(fullName + name + ".");
            dir = dirs.putIfAbsent(name, newDir);
            return dir == null ? newDir : dir;
        }

        private boolean createMetric(String name) {
            if (metrics.containsKey(name)) {
                return false;
            }
            MetricName newMetricName = new MetricName(fullName + name);
            return metrics.putIfAbsent(name, newMetricName) == null;
        }

        private MetricName getOrCreateMetric(String name) {
            MetricName metricName = metrics.get(name);
            if (metricName != null) {
                return metricName;
            }
            createMetric(name);
            return metrics.get(name);
        }

        @Override
        public String toString() {
            return "Dir{" +
                "fullName='" + fullName + '\'' +
                '}';
        }
    }

    private static class MetricName {
        private final String fullName;

        public MetricName(String fullName) {
            this.fullName = fullName;
        }

        private volatile boolean ban = false;

        @Override
        public String toString() {
            return "MetricName{" +
                "fullName='" + fullName + '\'' +
                '}';
        }
    }


}
