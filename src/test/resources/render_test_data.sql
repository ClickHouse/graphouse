INSERT INTO render_test.metrics (name, level, parent) VALUES ('dir1.', 1, '.');
INSERT INTO render_test.metrics (name, level, parent) VALUES ('dir1.metric1', 2, 'dir1.');
INSERT INTO render_test.metrics (name, level, parent) VALUES ('dir1.metric2', 2, 'dir1.');
INSERT INTO render_test.metrics (name, level, parent) VALUES ('dir1.metric3', 2, 'dir1.');
INSERT INTO render_test.metrics (name, level, parent) VALUES ('dir2.', 1, '.');
INSERT INTO render_test.metrics (name, level, parent) VALUES ('dir2.metric1', 2, 'dir2.');
INSERT INTO render_test.metrics (name, level, parent) VALUES ('dir2.metric2', 2, 'dir2.');
INSERT INTO render_test.metrics (name, level, parent) VALUES ('dir2.metric3', 2, 'dir2.');

INSERT INTO render_test.data (metric, value, timestamp, date, updated) VALUES
    ('dir1.metric1', 0, 0, '1970-01-01', 42),
    ('dir1.metric1', 1, 1, '1970-01-01', 42),
    ('dir1.metric1', 2, 2, '1970-01-01', 42),
    ('dir1.metric1', 3, 3, '1970-01-01', 42),
    ('dir1.metric1', 4, 4, '1970-01-01', 42),
    ('dir1.metric1', 5, 5, '1970-01-01', 42),
    ('dir1.metric2', 0, 0, '1970-01-01', 42),
    ('dir1.metric2', 2, 1, '1970-01-01', 42),
    ('dir1.metric2', 4, 2, '1970-01-01', 42),
    ('dir1.metric2', 6, 3, '1970-01-01', 42),
    ('dir1.metric2', 8, 4, '1970-01-01', 42),
    ('dir1.metric2', 10, 5, '1970-01-01', 42),
    ('dir1.metric3', 0, 0, '1970-01-01', 42),
    ('dir1.metric3', 2, 1, '1970-01-01', 42),
    ('dir1.metric3', 4, 2, '1970-01-01', 42),
    ('dir1.metric3', 8, 3, '1970-01-01', 42),
    ('dir1.metric3', 16, 4, '1970-01-01', 42),
    ('dir1.metric3', 32, 5, '1970-01-01', 42);

INSERT INTO render_test.data (metric, value, timestamp, date, updated) VALUES
    ('dir2.metric1', 1, 1, '1970-01-01', 42),
    ('dir2.metric1', 3, 3, '1970-01-01', 42),
    ('dir2.metric1', 5, 5, '1970-01-01', 42),
    ('dir2.metric2', 0, 0, '1970-01-01', 42),
    ('dir2.metric2', 4, 2, '1970-01-01', 42),
    ('dir2.metric2', 8, 4, '1970-01-01', 42),
    ('dir2.metric3', 2, 1, '1970-01-01', 42),
    ('dir2.metric3', 4, 2, '1970-01-01', 42),
    ('dir2.metric3', 8, 3, '1970-01-01', 42);
