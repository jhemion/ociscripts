import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MoveFilesNioMultiThreadAll {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java MoveFilesNioMultiThreadAll <sourceDir> <targetDir> " +
                    "[--overwrite] [--progressEvery=N] [--deleteEmptyDirs] [--threads=N] " +
                    "[--scanEverySeconds=N] [--stableSeconds=N]");
            System.exit(2);
        }

        Path sourceDir = Paths.get(args[0]).toAbsolutePath().normalize();
        Path targetDir = Paths.get(args[1]).toAbsolutePath().normalize();

        boolean overwrite = false;
        long progressEvery = 30_000L;
        boolean deleteEmptyDirs = false;
        int threads = 32;
        long scanEverySeconds = 60;
        long stableSeconds = 30;

        for (int i = 2; i < args.length; i++) {
            String a = args[i];
            if ("--overwrite".equalsIgnoreCase(a)) {
                overwrite = true;
            } else if (a.startsWith("--progressEvery=")) {
                progressEvery = Long.parseLong(a.substring("--progressEvery=".length()));
            } else if ("--deleteEmptyDirs".equalsIgnoreCase(a)) {
                deleteEmptyDirs = true;
            } else if (a.startsWith("--threads=")) {
                threads = Integer.parseInt(a.substring("--threads=".length()));
            } else if (a.startsWith("--scanEverySeconds=")) {
                scanEverySeconds = Long.parseLong(a.substring("--scanEverySeconds=".length()));
            } else if (a.startsWith("--stableSeconds=")) {
                stableSeconds = Long.parseLong(a.substring("--stableSeconds=".length()));
            } else {
                System.err.println("Unknown arg: " + a);
                System.exit(2);
            }
        }

        if (threads <= 0) throw new IllegalArgumentException("--threads must be > 0");
        if (scanEverySeconds <= 0) throw new IllegalArgumentException("--scanEverySeconds must be > 0");
        if (stableSeconds < 0) throw new IllegalArgumentException("--stableSeconds must be >= 0");
        if (!Files.isDirectory(sourceDir)) {
            throw new IllegalArgumentException("Source is not a directory: " + sourceDir);
        }
        if (Files.exists(targetDir) && !Files.isDirectory(targetDir)) {
            throw new IllegalArgumentException("Target exists but is not a directory: " + targetDir);
        }
        Files.createDirectories(targetDir);

        final boolean targetInsideSource = targetDir.startsWith(sourceDir);
        final CopyOption[] moveOptions = overwrite
                ? new CopyOption[]{StandardCopyOption.REPLACE_EXISTING}
                : new CopyOption[]{};
        final long progressEveryFinal = progressEvery;
        final boolean deleteEmptyDirsFinal = deleteEmptyDirs;
        final long stableMillis = TimeUnit.SECONDS.toMillis(stableSeconds);

        final AtomicLong movedCount = new AtomicLong(0);
        final AtomicLong failedCount = new AtomicLong(0);

        // Track directories so we can delete empty ones after moves (optional)
        final ConcurrentLinkedQueue<Path> dirsSeen = new ConcurrentLinkedQueue<>();

        // Prevent duplicate scheduling of the same file
        final Set<Path> inFlight = ConcurrentHashMap.newKeySet();

        int queueCapacity = Math.max(threads * 1024, 4096);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "move-worker");
            t.setDaemon(false);
            return t;
        };

        ThreadPoolExecutor moveExec = new ThreadPoolExecutor(
                threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueCapacity),
                tf,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
                Math.min(4, Math.max(1, threads / 8)),
                r -> {
                    Thread t = new Thread(r, "move-scheduler");
                    t.setDaemon(false);
                    return t;
                }
        );

        System.out.println("Starting continuous mover. threads=" + threads +
                ", overwrite=" + overwrite +
                ", deleteEmptyDirs=" + deleteEmptyDirsFinal +
                ", progressEvery=" + progressEveryFinal +
                ", scanEverySeconds=" + scanEverySeconds +
                ", stableSeconds=" + stableSeconds +
                ", source=" + sourceDir + ", target=" + targetDir);

        // Shutdown hook for clean stop
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown requested...");
            scheduler.shutdownNow();
            moveExec.shutdownNow();
        }, "shutdown-hook"));

        // --- Helpers ---

        // Ensure destination directory exists
        Runnable deleteEmptyDirsRun = () -> {
            if (!deleteEmptyDirsFinal) return;
            // Delete deepest-first
            ArrayList<Path> dirs = new ArrayList<>(new HashSet<>(dirsSeen));
            dirs.sort((a, b) -> Integer.compare(b.getNameCount(), a.getNameCount()));
            for (Path d : dirs) {
                Path nd = d.toAbsolutePath().normalize();
                if (nd.equals(sourceDir)) continue;
                if (targetInsideSource && nd.startsWith(targetDir)) continue;
                try {
                    Files.delete(nd); // only if empty
                } catch (IOException ignored) {
                }
            }
        };

        // Schedule/attempt a move with stability checking.
        // If the file is still changing, it will retry later.
        class Mover {
            void scheduleMove(Path nf) {
                Path p = nf.toAbsolutePath().normalize();
                if (targetInsideSource && p.startsWith(targetDir)) return;
                if (!inFlight.add(p)) return; // already scheduled/in progress

                scheduler.execute(() -> attemptMove(p, 0));
            }

            void attemptMove(Path nf, int attempts) {
                try {
                    if (!Files.exists(nf) || Files.isDirectory(nf)) {
                        inFlight.remove(nf);
                        return;
                    }

                    // Stability check: unchanged size + mtime for stableMillis
                    if (stableMillis > 0) {
                        BasicFileAttributes a1;
                        try {
                            a1 = Files.readAttributes(nf, BasicFileAttributes.class);
                        } catch (NoSuchFileException e) {
                            inFlight.remove(nf);
                            return;
                        }

                        long size1 = a1.size();
                        FileTime mt1 = a1.lastModifiedTime();

                        scheduler.schedule(() -> {
                            try {
                                if (!Files.exists(nf) || Files.isDirectory(nf)) {
                                    inFlight.remove(nf);
                                    return;
                                }
                                BasicFileAttributes a2 = Files.readAttributes(nf, BasicFileAttributes.class);
                                long size2 = a2.size();
                                FileTime mt2 = a2.lastModifiedTime();

                                if (size1 == size2 && mt1.equals(mt2)) {
                                    // Looks stable -> perform move on moveExec
                                    submitMove(nf);
                                } else {
                                    // Still changing -> retry
                                    // (keep inFlight set so we don't schedule duplicates)
                                    attemptMove(nf, attempts + 1);
                                }
                            } catch (Exception e) {
                                // If anything goes wrong, back off a bit and retry a few times
                                if (attempts < 20) {
                                    scheduler.schedule(() -> attemptMove(nf, attempts + 1),
                                            Math.max(1000, stableMillis), TimeUnit.MILLISECONDS);
                                } else {
                                    failedCount.incrementAndGet();
                                    System.err.println("FAILED (stability retries exceeded): " + nf + " | " + e);
                                    inFlight.remove(nf);
                                }
                            }
                        }, stableMillis, TimeUnit.MILLISECONDS);

                    } else {
                        // No stability window requested
                        submitMove(nf);
                    }

                } catch (Exception e) {
                    failedCount.incrementAndGet();
                    System.err.println("FAILED (schedule/attempt): " + nf + " | " + e);
                    inFlight.remove(nf);
                }
            }

            void submitMove(Path nf) {
                Path rel = sourceDir.relativize(nf);
                Path dest = targetDir.resolve(rel);

                moveExec.execute(() -> {
                    try {
                        Path parent = dest.getParent();
                        if (parent != null) Files.createDirectories(parent);

                        // If file got moved already, or disappeared, ignore quietly
                        if (!Files.exists(nf)) return;

                        Files.move(nf, dest, moveOptions);

                        long mc = movedCount.incrementAndGet();
                        if (progressEveryFinal > 0 && (mc % progressEveryFinal == 0)) {
                            System.out.println("Moved " + mc + " files...");
                        }
                    } catch (IOException e) {
                        failedCount.incrementAndGet();
                        System.err.println("FAILED to move: " + nf + " -> " + dest +
                                " | " + e.getClass().getSimpleName() +
                                (e.getMessage() != null ? (": " + e.getMessage()) : ""));
                    } catch (RuntimeException e) {
                        failedCount.incrementAndGet();
                        System.err.println("FAILED (runtime) to move: " + nf + " -> " + dest +
                                " | " + e.getClass().getSimpleName() +
                                (e.getMessage() != null ? (": " + e.getMessage()) : ""));
                    } finally {
                        inFlight.remove(nf);
                    }
                });
            }
        }

        Mover mover = new Mover();

        // Scan once (and also used by the periodic scan)
        Runnable scanOnce = () -> {
            try {
                Files.walkFileTree(sourceDir, EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE,
                        new SimpleFileVisitor<>() {
                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                                Path nd = dir.toAbsolutePath().normalize();
                                if (targetInsideSource && nd.startsWith(targetDir)) return FileVisitResult.SKIP_SUBTREE;

                                dirsSeen.add(nd);

                                Path rel = sourceDir.relativize(nd);
                                Path destDir = targetDir.resolve(rel);
                                Files.createDirectories(destDir);

                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                                Path nf = file.toAbsolutePath().normalize();
                                if (targetInsideSource && nf.startsWith(targetDir)) return FileVisitResult.CONTINUE;
                                if (!attrs.isRegularFile()) return FileVisitResult.CONTINUE;

                                mover.scheduleMove(nf);
                                return FileVisitResult.CONTINUE;
                            }
                        });
            } catch (Exception e) {
                System.err.println("Scan failed: " + e);
            } finally {
                // Optional cleanup (best-effort) after a scan cycle
                deleteEmptyDirsRun.run();
            }
        };

        // --- WatchService (recursive) ---
        WatchService watchService = FileSystems.getDefault().newWatchService();
        Map<WatchKey, Path> keyToDir = new ConcurrentHashMap<>();

        // Register all existing dirs initially
        Files.walkFileTree(sourceDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path nd = dir.toAbsolutePath().normalize();
                if (targetInsideSource && nd.startsWith(targetDir)) return FileVisitResult.SKIP_SUBTREE;

                dirsSeen.add(nd);

                WatchKey key = nd.register(watchService,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY);
                keyToDir.put(key, nd);
                return FileVisitResult.CONTINUE;
            }
        });

        // Periodic scan every minute
        scheduler.scheduleWithFixedDelay(scanOnce, 0, scanEverySeconds, TimeUnit.SECONDS);

        // Dedicated watcher loop thread
        Thread watcherThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                WatchKey key;
                try {
                    key = watchService.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (ClosedWatchServiceException e) {
                    break;
                }

                Path dir = keyToDir.get(key);
                if (dir == null) {
                    key.reset();
                    continue;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    if (kind == StandardWatchEventKinds.OVERFLOW) continue;

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path name = ev.context();
                    Path child = dir.resolve(name).toAbsolutePath().normalize();

                    if (targetInsideSource && child.startsWith(targetDir)) continue;

                    try {
                        if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                            // If new directory, register it (and its subdirs)
                            if (Files.isDirectory(child)) {
                                Files.walkFileTree(child, new SimpleFileVisitor<>() {
                                    @Override
                                    public FileVisitResult preVisitDirectory(Path d, BasicFileAttributes attrs) throws IOException {
                                        Path nd = d.toAbsolutePath().normalize();
                                        if (targetInsideSource && nd.startsWith(targetDir)) return FileVisitResult.SKIP_SUBTREE;

                                        dirsSeen.add(nd);
                                        WatchKey k = nd.register(watchService,
                                                StandardWatchEventKinds.ENTRY_CREATE,
                                                StandardWatchEventKinds.ENTRY_MODIFY);
                                        keyToDir.put(k, nd);
                                        return FileVisitResult.CONTINUE;
                                    }
                                });
                            } else {
                                mover.scheduleMove(child);
                            }
                        } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                            // Modifies can indicate a file finished writing; schedule a move attempt
                            if (Files.isRegularFile(child)) {
                                mover.scheduleMove(child);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Watch handling failed for " + child + ": " + e);
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    keyToDir.remove(key);
                }
            }
        }, "watch-service-loop");

        watcherThread.setDaemon(false);
        watcherThread.start();

        // Keep main alive
        watcherThread.join();
    }
}