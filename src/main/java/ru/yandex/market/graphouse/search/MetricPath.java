package ru.yandex.market.graphouse.search;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;

/**
 * Implementation of Path whose only purpose is to have a correct toString implementation
 * It is then used by {@link java.nio.file.PathMatcher}\s path matching mechanism.
 *
 * @author Maksim Leonov (nohttp@)
 */
public class MetricPath implements Path {
    private final String metricPath;

    public MetricPath(String metricPath) {
        this.metricPath = metricPath;
    }

    @Override
    public String toString() {
        return metricPath;
    }

    @Override
    public FileSystem getFileSystem() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public boolean isAbsolute() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path getRoot() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path getFileName() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path getParent() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public int getNameCount() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path getName(int index) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public boolean startsWith(Path other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public boolean startsWith(String other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public boolean endsWith(Path other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public boolean endsWith(String other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path normalize() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path resolve(Path other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path resolve(String other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path resolveSibling(Path other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path resolveSibling(String other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path relativize(Path other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public URI toUri() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path toAbsolutePath() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events) throws IOException {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public Iterator<Path> iterator() {
        throw new UnsupportedOperationException("Unexpected call!");
    }

    @Override
    public int compareTo(Path other) {
        throw new UnsupportedOperationException("Unexpected call!");
    }
}
